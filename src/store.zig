const std = @import("std");
const time = std.time;
pub const RedisStore = struct {
    table: std.StringHashMap(RedisVal),
    db_index: ?usize = null,

    const RedisVal = struct {
        val: []const u8,
        expiry: ?i64 = null,
        type: Type,

        const Type = enum { string };
    };

    pub fn init(alloc: std.mem.Allocator) !*RedisStore {
        const store = try alloc.create(RedisStore);
        store.table = std.StringHashMap(RedisVal).init(alloc);

        return store;
    }

    pub fn deinit(self: *RedisStore) void {
        self.table.deinit();
    }

    pub fn get(self: *RedisStore, key: []const u8) !RedisVal {
        if (self.table.get(key)) |v| {
            if (v.expiry) |exp| {
                const now = time.milliTimestamp();
                if (now < exp) {
                    return v;
                } else {
                    return error.KeyHasExceededExpirationThreshold;
                }
            }
            return v;
        } else {
            return error.NoValueExistforGivenKey;
        }
    }

    pub fn set(
        self: *RedisStore,
        key: []const u8,
        val: []const u8,
        exp: ?i64,
        exp_timestamp: ?i64,
    ) !void {
        var rv = RedisVal{ .val = val, .type = .string };

        if (exp_timestamp) |exp_ts| {
            rv.expiry = exp_ts;
        } else if (exp) |e| {
            const now = time.milliTimestamp();

            rv.expiry = now + e;
        }
        try self.table.put(key, rv);
    }
};
