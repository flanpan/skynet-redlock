local conf = {

    -- redis服务器信息, redis服务器数为奇数
    -- 最好将奇数台reids部署在不同机器, 达到容错、高可用目的
    servers = {
        { host = "127.0.0.1", port = 6379, db = 0, auth = "123456"}
    },

    -- uuid生成器, 请替换成真uuid(可使用uuid库uuid_generate)
    make_uuid = function()
        local fn = function(x)
            local r = math.random(16) - 1
            r = (x == "x") and (r + 1) or (r % 4) + 9
            return ("0123456789abcdef"):sub(r, r)
        end
        return (("xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"):gsub("[xy]", fn))
    end,

    -- 默认锁过期时间
    timeout = 3000,

    -- 用计算服务器漂移时间 (http://redis.io/topics/distlock)
    drift_factor = 0.01,

    -- 重试获得锁最大次数
    retry_count = 10,

    -- 重试获得锁间隔(毫秒)
    retry_delay = 200,

    -- 重试获得锁抖动时间(毫秒) https://www.awsarchitectureblog.com/2015/03/backoff.html
    retry_jitter = 100,

    -- 请求redis超时时间(毫秒)
    request_timeout = 500,

    -- 最大会话数量
    max_session = 20000,
}

return conf

