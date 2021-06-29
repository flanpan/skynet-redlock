local skynet = require "skynet"
local redlock = {}

local addr
local make_uuid

local function default_cb() end


local function _lock(lockname, func, cb, timeout, ...)
    cb = cb or default_cb
    local uuid = make_uuid()
    local ok, data = skynet.call(addr, "lua", "lock", lockname, uuid, timeout)
    if not ok then
        return cb(ok, data)
    end
    cb(pcall(func, ...))
    skynet.call(addr, "lua", "unlock", lockname, uuid)
end

function redlock.lock(lockname, func, cb, timeout, ...)
    skynet.fork(_lock, lockname, func, cb, timeout, ...)
end

skynet.init(function()
    local conf = require "redlock_conf"
    make_uuid = conf.make_uuid
    addr = skynet.uniqueservice("redlockd")
end)

return redlock