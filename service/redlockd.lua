local skynet = require "skynet"
local redis = require "skynet.db.redis"
local crypt = require "skynet.crypt"

local traceback = debug.traceback
local tpack = table.pack
local tunpack = table.unpack
local tconcat = table.concat
local math_floor = math.floor
local math_ceil = math.ceil
local math_random = math.random

local function hash(script)
    local key = crypt.sha1(script)
    return crypt.hexencode(key)
end

local QUORUM

local SCRIPT = {
    LOCK = [[
        local key = KEYS[1]
        local uuid = ARGV[1]
        if redis.call("exists", key) == 1 then
            return
        end
        redis.call("set", key, uuid, "PX", ARGV[2])
        return uuid
    ]],
    UNLOCK = [[
        local key = KEYS[1]
        local uuid = redis.call("get", key)
        if uuid == ARGV[1] then
            redis.pcall("del", key)
        end
        return uuid
    ]],
    EXTEND = [[
        local key = KEYS[1]
        local uuid = ARGV[1]
        if redis.call("get", key) ~= uuid then
            return
        end
        redis.call("set", key, uuid, "PX", ARGV[2])
        return uuid
    ]]
}

local SCRIPT_HASH = {
    LOCK = hash(SCRIPT.LOCK),
    UNLOCK = hash(SCRIPT.UNLOCK),
    EXTEND = hash(SCRIPT.EXTEND)
}

local conf
local dbs = {}
local sessions = {}
local session_cnt = 0
local NORET = {}

local function execute_script(db, type, s)
    local timeout = conf.timeout
    local lockname = s.lockname
    local uuid = s.uuid
    local ok, ret = pcall(db["evalsha"], db, SCRIPT_HASH[type], 1, lockname, uuid, timeout)
    if not ok and ret:find("NOSCRIPT") then
        ok, ret = pcall(db["eval"], db, SCRIPT[type], 1, lockname, uuid, timeout)
    end

    if not ok then
        skynet.error("redis execute_script err.", ret, lockname, uuid, timeout)
        return false
    end

    if ret == uuid then
        return true
    end
    return false
end

local function execute_script_timeout(db, type, s)
    local co = coroutine.running()
    local ok, ret = false, "timeout"

    skynet.fork(function()
        ok, ret = execute_script(db, type, s)
        if co then
            skynet.wakeup(co)
            co = nil
        end
    end)

    skynet.sleep(conf.request_timeout/10)
    if co then
        co = nil
    end
    return ok, ret
end

local function calc_time(s)
    local timeout = conf.timeout
    local now = skynet.now()*10
    local drift = math_floor(conf.drift_factor * timeout) + 2
    s.starttime = now
    s.endtime = now + timeout - drift
end

local function make_session(lockname, uuid, hold)
    assert(not sessions[uuid])
    local s = {
        lockname = lockname,
        uuid = uuid,
        attempts = 0,
        starttime = 0,
        endtime = 0,
        hold = hold
    }
    calc_time(s)
    sessions[uuid] = s
    session_cnt = session_cnt + 1
    return s
end

local function free_session(s)
    s.endtime = 0
    if sessions[s.uuid] then
        sessions[s.uuid] = nil
        session_cnt = session_cnt - 1
    end
end


local function unlock(s)
    if not s then
        return false, "session not exist"
    end

    local votes = 0
    for _, db in pairs(dbs) do
        if execute_script(db, "UNLOCK", s) then
            votes = votes + 1
        end
    end
    if votes < QUORUM then
        skynet.error("redlockd lock expired", s.uuid)
        return false, "expired"
    end
    return true
end

local function attempt(s, is_extend)
    s.attempts = s.attempts + 1
    local votes = 0
    for _, db in pairs(dbs) do
        local ok = false
        if is_extend then
            ok = execute_script_timeout(db, "EXTEND", s)
        else
            ok = execute_script_timeout(db, "LOCK", s)
        end
        if ok then
            votes = votes + 1
        end
    end
    
    local now = skynet.now()*10
    if votes >= QUORUM and s.endtime > now then
        local ti = conf.timeout/3 - (now-s.starttime)
        ti = math_floor(ti/10)
        if ti < 0 then
            ti = 0
        end
        skynet.timeout(ti, function()
            if s.endtime == 0 then
                return
            end
            s.attempts = 0
            calc_time(s)
            attempt(s, true)
        end)
        return true
    else
        -- retry
        unlock(s)
        if s.hold or s.attempts <= conf.retry_count then
            local t = conf.retry_delay + math_floor((math_random()*2-1) * conf.retry_jitter)
            t = math_ceil(t/10)
            skynet.sleep(t)
            if s.endtime == 0 then
                return false, "be unlocked"
            end
            calc_time(s)
            return attempt(s)
        end
        -- failed
        free_session(s)
        return false, "timeout"
    end
end


local CMD = {}

function CMD.lock(lockname, uuid, hold)
    local s = sessions[uuid]
    if s then
        return false, "session exist"
    end

    if session_cnt >= conf.max_session then
        return false, "session limit"
    end

    s = make_session(lockname, uuid, hold)
    return attempt(s)
end

function CMD.unlock(lockname, uuid)
    local s = sessions[uuid]
    if not s then
        return false, "session not exist"
    end
    local ok, data = unlock(s)
    free_session(s)
    return ok, data
end

function CMD.unlock_batch(uuid2lockname)
    for uuid, lockname in pairs(uuid2lockname) do
        CMD.unlock(lockname, uuid)
    end
    return NORET
end

skynet.init(function()
    conf = require "redlock_conf"
    for _,client in ipairs(conf.servers) do
        table.insert(dbs, redis.connect(client))
    end
    QUORUM = math_floor(#conf.servers / 2) + 1
end)


skynet.start(function()
    skynet.dispatch("lua", function(_, addr, cmd, ...)
        local f = CMD[cmd]
        assert(f, cmd)
        local ok, data = f(...)
        if ok ~= NORET then
            skynet.retpack(ok, data)
        end
    end)
end)

