local skynet = require "skynet"

local addr
local make_uuid

local uuid2lockname = {}

local redlock = setmetatable({}, {
    __gc = function(self)
        if next(uuid2lockname) then
            skynet.send(addr, "lua", "unlock_batch", uuid2lockname)
        end
    end
})

local function default_cb() end

local function _lock(lockname, func, cb, hold, ...)
    cb = cb or default_cb
    local uuid = make_uuid()
    uuid2lockname[uuid] = lockname

    local ok, data = skynet.call(addr, "lua", "lock", lockname, uuid, hold)
    if not ok then
        uuid2lockname[uuid] = nil
        return cb(ok, data)
    end

    cb(pcall(func, ...))
    local ok, data = skynet.call(addr, "lua", "unlock", lockname, uuid)
    if not ok then
        skynet.error("redlock err.", data, uuid)
    end
    uuid2lockname[uuid] = nil
end

function redlock.lock(lockname, func, cb, ...)
    skynet.fork(_lock, lockname, func, cb, false, ...)
end

function redlock.holdlock(lockname, func, cb, ...)
    skynet.fork(_lock, lockname, func, cb, true, ...)
end

skynet.init(function()
    local conf = require "redlock_conf"
    make_uuid = conf.make_uuid
    addr = skynet.uniqueservice("redlockd")
end)

return redlock
