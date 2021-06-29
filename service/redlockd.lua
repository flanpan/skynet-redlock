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
        if redis.call("exists", key) == 1 then
            return 0
        end
        redis.call("set", key, ARGV[1], "PX", ARGV[2])
        return 1
    ]],
    UNLOCK = [[
        local key = KEYS[1]
        if redis.call("get", key) == ARGV[1] then
            redis.pcall("del", key)
            return 1
        end
        return 0
    ]],
    EXTEND = [[
        local key = KEYS[1]
        if redis.call("get", key) ~= ARGV[1] then
            return 0
        end
        redis.call("set", key, ARGV[1], "PX", ARGV[2])
        return 1
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

local function execute_script(db, type, s)
    local ok, ret = pcall(db["evalsha"], db, SCRIPT_HASH[type], 1, s.lockname, s.uuid, s.timeout)
    if not ok and ret:find("NOSCRIPT") then
        ok, ret = pcall(db["eval"], db, SCRIPT[type], 1, s.lockname, s.uuid, s.timeout)
    end
    if not ok then
        skynet.error("redis execute_script err.", ret, s.lockname, s.uuid, s.timeout)
        return false
    end
    if ret == 1 then
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
    local now = skynet.now()*10
    local drift = math_floor(conf.drift_factor * s.timeout) + 2
    s.starttime = now
    s.endtime = now + s.timeout - drift
end

local function make_session(lockname, uuid, timeout)
    local s = {
        lockname = lockname,
        uuid = uuid,
        timeout = timeout,
        attempts = 0,
        starttime = 0,
        endtime = 0,
    }
    calc_time(s)
    return s
end

local function unlock(s)
    s.endtime = 0
    for _, db in pairs(dbs) do
        execute_script(db, "UNLOCK", s)
    end
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
        local ti = s.timeout/3 - (now-s.starttime)
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
        unlock(s)
        -- retry
        if conf.retry_count == -1 or s.attempts <= conf.retry_count then
            local t = conf.retry_delay + math_floor((math_random()*2-1) * conf.retry_jitter)
            skynet.sleep(math_ceil(t/10))
            calc_time(s)
            return attempt(s)
        end
        -- failed
        sessions[s.uuid] = nil
        return false, "timeout"
    end
end


local CMD = {}

function CMD.lock(lockname, uuid, timeout)
    local timeout = timeout or conf.timeout
    local s = sessions[uuid]
    if s then
        return false, "session exist"
    end
    s = make_session(lockname, uuid, timeout)
    sessions[uuid] = s

    return attempt(s)
end

function CMD.unlock(lockname, uuid)
    local s = sessions[uuid]
    if not s then
        return false, "session not exist."
    end
    sessions[uuid] = nil
    return unlock(s)
end


skynet.init(function()
    conf = require "redlock_conf"
    for _,client in ipairs(conf.servers) do
        table.insert(dbs, redis.connect(client))
    end
    QUORUM = math_floor(#conf.servers / 2) + 1
end)


skynet.start(function()
    skynet.dispatch("lua", function(_, _, cmd, ...)
        local f = CMD[cmd]
        assert(f, cmd)
        skynet.retpack(f(...))
    end)
end)

