local skynet = require "skynet"
local redlock = require "redlock"

local function test_lock1()
    skynet.error("test_lock")
end

local function test_lock2(p)
    skynet.error("test_lock2", p)
    return p .." world"
end

local function test_cb(ok, ret)
    assert(ok, ret)
    skynet.error(ok, ret)
end

local is_slave = ...

skynet.start(function()
    redlock.lock("lock:test1", test_lock1)
    redlock.lock("lock:test1", test_lock1, test_cb)
    redlock.lock("lock:test1", test_lock2, test_cb, 3000, "hello")
    if not is_slave then
        for i=1, 10 do
            skynet.newservice("test_redlock", "i am slave")
        end
    end

end)

