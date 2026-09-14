package valkey

import "github.com/valkey-io/valkey-go"

// Lua scripts inlined from the former scripts/ directory. Each script is
// compiled into a valkey.Lua which uses EVALSHA/EVAL under the hood.

const tokenBucketLua = `local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local ttl
if rate <= 0 then
  ttl = 3600
else
  ttl = math.ceil(capacity / rate) + 10
end

local n = #KEYS
local refilled = {}
local allowed = 1
local max_wait = 0

for i = 1, n do
  local data    = redis.call('HMGET', KEYS[i], 'tokens', 'last_ms')
  local tokens  = tonumber(data[1])
  local last_ms = tonumber(data[2])

  if tokens == nil then tokens = capacity end
  if last_ms == nil then last_ms = now_ms end

  local elapsed = math.max(0, now_ms - last_ms) / 1000
  tokens = math.min(capacity, tokens + elapsed * rate)

  refilled[i] = tokens

  if tokens < requested then
    allowed = 0
    local wait
    if rate <= 0 then
      wait = ttl * 1000
    else
      wait = math.ceil((requested - tokens) / rate * 1000)
    end
    if wait > max_wait then max_wait = wait end
  end
end

for i = 1, n do
  local tokens = refilled[i]
  if allowed == 1 then
    tokens = tokens - requested
  end
  redis.call('HMSET', KEYS[i], 'tokens', tokens, 'last_ms', now_ms)
  redis.call('EXPIRE', KEYS[i], ttl)
end

return { allowed, max_wait }`

const cbTransitionLua = `local key      = KEYS[1]
local to       = ARGV[1]
local cooldown = tonumber(ARGV[2])
local fails    = tonumber(ARGV[3])

local raw  = redis.call('GET', key)
local cb   = raw and cjson.decode(raw) or {}
local from = cb.state or 'CLOSED'

local ok = (from == 'CLOSED'    and to == 'OPEN')
        or (from == 'OPEN'      and to == 'HALF_OPEN')
        or (from == 'HALF_OPEN' and (to == 'CLOSED' or to == 'OPEN'))

if not ok then return 0 end

cb.state = to
cb.cooldown_until = cooldown
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return 1`

const cbRecordFailureLua = `local key       = KEYS[1]
local threshold = tonumber(ARGV[1])
local now_epoch = tonumber(ARGV[2])

local raw   = redis.call('GET', key)
local cb    = raw and cjson.decode(raw) or {}
local state = cb.state or 'CLOSED'
local fails = (cb.consecutive_failures or 0) + 1
local opened = 0

if state == 'HALF_OPEN' or (state == 'CLOSED' and fails >= threshold) then
  state = 'OPEN'
  local secs = math.floor(60 * (2 ^ (fails - 1)))
  if secs > 240 then secs = 240 end
  cb.cooldown_until = now_epoch + secs
  opened = 1
end

cb.state = state
cb.consecutive_failures = fails
redis.call('SET', key, cjson.encode(cb))
return {opened, fails}`

const cbRecordSuccessLua = `local key = KEYS[1]

local raw   = redis.call('GET', key)
local cb    = raw and cjson.decode(raw) or {}
local state = cb.state or 'CLOSED'

if state == 'HALF_OPEN' then
  state = 'CLOSED'
end

cb.state = state
cb.consecutive_failures = 0
cb.cooldown_until = 0
redis.call('SET', key, cjson.encode(cb))
return cb.state`

const cbSetScoreLua = `local key   = KEYS[1]
local score = tonumber(ARGV[1])

local raw = redis.call('GET', key)
local cb  = raw and cjson.decode(raw) or {}

cb.last_known_score = score
cb.state = cb.state or 'CLOSED'
redis.call('SET', key, cjson.encode(cb))
return cb.state`

var (
	tokenBucketScript     = valkey.NewLuaScript(tokenBucketLua)
	cbTransitionScript    = valkey.NewLuaScript(cbTransitionLua)
	cbRecordFailureScript = valkey.NewLuaScript(cbRecordFailureLua)
	cbRecordSuccessScript = valkey.NewLuaScript(cbRecordSuccessLua)
	cbSetScoreScript      = valkey.NewLuaScript(cbSetScoreLua)
)
