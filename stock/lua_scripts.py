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
