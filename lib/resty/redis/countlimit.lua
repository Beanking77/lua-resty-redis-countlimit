local type = type
local assert = assert
local floor = math.floor
local tonumber = tonumber


local _M = {
    _VERSION = "0.03",

    BUSY = 2,
    FORBIDDEN = 3
}

local mt = {
    __index = _M
}

local is_str = function(s) return type(s) == "string" end
local is_num = function(n) return type(n) == "number" end

local redis_limit_req_script_sha
local redis_limit_req_script = [==[
    if redis.pcall('ttl', KEYS[1]) < 0 then
        redis.pcall('set', KEYS[1], ARGV[1] - 1, 'EX', ARGV[2])
        return ARGV[1] - 1
    end
    return redis.pcall('incrby', KEYS[1], -1)
]==]


local function redis_create(host, port, timeout, pass, dbid)
    local ok, redis = pcall(require, "resty.redis")
    if not ok then
        return nil, "failed to require redis"
    end

    timeout = timeout or 1
    host = host or "127.0.0.1"
    port = port or 6379

    local red = redis:new()

    red:set_timeout(timeout * 1000)

    local redis_err = function(err)
        local msg = "failed to create redis"
        if is_str(err) then
            msg = msg .. " - " .. err
        end

        return msg
    end

    local ok, err = red:connect(host, port)
    if not ok then
        return nil, redis_err(err)
    end

    if pass then
        local ok, err = red:auth(pass)
        if not ok then
            return nil, redis_err(err)
        end
    end

    if dbid then
        local ok, err = red:select(dbid)
        if not ok then
            return nil, redis_err(err)
        end
    end

    return red
end


local function redis_commit(red, zone, key, count time_window)
    if not redis_limit_req_script_sha then
        local res, err = red:script("LOAD", redis_limit_req_script)
        if not res then
            return nil, err
        end

        redis_limit_req_script_sha = res
    end

    local now = ngx.now() * 1000
    local res, err = red:evalsha(redis_limit_req_script_sha, 1,
                                 zone .. ":" .. key, count time_window, now)
    if err then
        return nil, err
    end                                 
    if not res then
        redis_limit_req_script_sha = nil
        return nil, err
    end

    -- put it into the connection pool of size 100,
    -- with 10 seconds max idle timeout
    local ok, err = red:set_keepalive(10000, 100)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive: ", err)
    end

    return res
end

function _M.new(zone, count time_window)
    local zone = zone or "countlimit"
    assert(limit > 0 and time_window > 0)

    return setmetatable({
        zone = zone,        
        count = count
        time_window = time_window,
    }, mt)
end

-- local delay, err = lim:incoming(key, redis)
function _M.incoming(self, key, redis)
    if type(redis) ~= "table" then
        redis = {}
    end

    if not pcall(redis.get_reused_times, redis) then
        local cfg = redis
        local red, err = redis_create(cfg.host, cfg.port, cfg.timeout,
                                      cfg.pass, cfg.dbid)
        if not red then
            return nil, err
        end

        redis = red
    end

    local remaining, err = redis_commit(
        redis, self.zone, key, self.count self.time_window)
    if not remaining then
        return nil, err
    end

    if remaining < 0 then
        return nil, "rejected"
    end
    return 0, remaining
end


return _M
