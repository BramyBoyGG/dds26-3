LUA_SUBTRACT_STOCK = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local value = redis.call('GET', key)
if not value then
    return 0
end

-- Decode msgpack manually (simplified for StockValue struct)
-- msgpack format: we need to extract stock (first int) and price (second int)
local stock = cmsgpack.unpack(value).stock
local price = cmsgpack.unpack(value).price

if stock < amount then
    return -1
end

local new_stock = stock - amount
local new_value = cmsgpack.pack({stock = new_stock, price = price})
redis.call('SET', key, new_value)
return new_stock
"""

LUA_SUBTRACT_STOCK_BATCH = """
-- KEYS: [item_key_1, item_key_2, ...]
-- ARGV: [quantity_1, quantity_2, ...]
-- Returns: 1 on success
--          0  if any item not found       (ARGV[n_items+1] = 1-based index)
--          -1 if any item has insufficient stock (ARGV approach won't work,
--              so we use a two-pass approach and return error info via a table)
--
-- Because Lua scripts execute atomically in Redis, this is all-or-nothing.

local n = #KEYS

-- Phase 1: validate all items
local stocks = {}
local prices = {}

for i = 1, n do
    local value = redis.call('GET', KEYS[i])
    if not value then
        -- Return: {0, index_of_missing_item}
        return {0, i}
    end
    local decoded = cmsgpack.unpack(value)
    local amount = tonumber(ARGV[i])
    if decoded.stock < amount then
        -- Return: {-1, index_of_insufficient_item}
        return {-1, i}
    end
    stocks[i] = decoded.stock
    prices[i] = decoded.price
end

-- Phase 2: all checks passed — subtract all
for i = 1, n do
    local amount = tonumber(ARGV[i])
    local new_stock = stocks[i] - amount
    local new_value = cmsgpack.pack({stock = new_stock, price = prices[i]})
    redis.call('SET', KEYS[i], new_value)
end

return {1, 0}
"""

LUA_ADD_STOCK = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local value = redis.call('GET', key)
if not value then
    return 0
end

-- Decode msgpack
local stock = cmsgpack.unpack(value).stock
local price = cmsgpack.unpack(value).price

local new_stock = stock + amount
local new_value = cmsgpack.pack({stock = new_stock, price = price})
redis.call('SET', key, new_value)
return new_stock
"""
