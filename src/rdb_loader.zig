const std = @import("std");
const RedisStore = @import("store.zig").RedisStore;

const StringVal = struct {
    val: []const u8,
    len: usize,
    type: StringType,

    const StringType = enum { string, integer };
};

const RdbLoader = struct {
    bytes: [:0]const u8 = undefined,
    index: usize = 0,
    store: RedisStore,
    db_index: usize,
    alloc: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, dirname: []const u8, filename: []const u8) !RdbLoader {
        const cwd = std.fs.cwd();
        const path = try std.fs.path.join(allocator, &[_][]const u8{ dirname, filename });
        defer allocator.free(path);

        const file = try cwd.openFile(path, .{ .mode = .read_only });
        defer file.close();

        const buffer = try allocator.allocSentinel(u8, try file.getEndPos(), 0);
        errdefer allocator.free(buffer);
        const bytes_read = try file.readAll(buffer);

        std.debug.print("RDBLOADER BYTES READ {any}\n", .{bytes_read});

        return .{ .alloc = allocator, .store = RedisStore.init(allocator), .bytes = buffer, .db_index = undefined };
    }

    pub fn deinit(self: *RdbLoader, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.store.deinit();
    }

    pub fn parse(self: *RdbLoader) !void {
        std.debug.assert(std.mem.containsAtLeast(u8, self.bytes[0..7], 1, "REDIS00"));

        self.index = 9;

        std.debug.print("WHAT SECTION {x}\n", .{self.bytes});
        while (true) {
            switch (self.bytes[self.index]) {
                0xfc | 0xFC => {
                    std.debug.print("FC SECTION \n", .{});
                    _ = self.next(); // consume fc op code
                    const expiry = self.bytes[self.index .. self.index + 8];

                    const parsed_expiry = std.mem.readVarInt(i64, expiry, .little);

                    std.debug.print("RDBLOADER FC SECTION - EXPIRATION {x}, {d}\n", .{ expiry, parsed_expiry });
                    self.index += 8;

                    continue;
                },
                0xfd | 0xFD => {
                    std.debug.print("FD SECTION \n", .{});
                    _ = self.next(); // consume fd op code
                    continue;
                },
                0xfe | 0xFE => {
                    std.debug.print("DB SECTION \n", .{});
                    _ = self.next(); // consume db op code
                    const len_byte = self.next(); // consume len byte

                    const val = try self.decode_length(len_byte);

                    self.db_index = val.len;
                },
                0x0 => {
                    std.debug.print("STRING VAL TYPE - 0x0\n", .{});
                    _ = self.next(); // consume value type

                    const str_key_prefix_byte = self.next(); // consume len prefixed string

                    const string_key = try self.decode_length(str_key_prefix_byte);
                    defer if (string_key.type == .integer) self.alloc.free(string_key.val);

                    std.debug.print("DECODE STRING: LEN: {d}, KEY: {s} \n", .{ string_key.len, string_key.val });

                    const str_val_prefix_byte = self.next(); // consume len prefixed string
                    //
                    const string_val = try self.decode_length(str_val_prefix_byte);
                    defer if (string_val.type == .integer) self.alloc.free(string_val.val);

                    std.debug.print("DECODE STRING: LEN: {d}, VAL: {s}\n", .{ string_val.len, string_val.val });
                },
                else => {
                    std.debug.print("UNKNOWN {x}\n", .{self.bytes[self.index]});
                    break;
                },
            }
        }
    }

    fn decode_length(self: *RdbLoader, byte: u8) !StringVal {
        const first_two_bits = byte >> 6;

        switch (first_two_bits) {
            0b00 => {
                std.debug.print("FIRST TWO BITS 0b00 - DECODE LENGTH\n", .{});
                const last_six_bits = byte & 0b00111111;
                const len = @as(usize, @intCast(last_six_bits));
                const val = self.bytes[self.index .. self.index + len];

                self.index += len;

                return StringVal{ .val = val, .len = len, .type = .string };
            },
            0b01 => {
                // TODO: fill in impl later
                std.debug.print("FIRST TWO BITS 0b01 - DECODE LENGTH\n", .{});
                return StringVal{ .val = "test", .len = 0, .type = .string };
            },
            0b10 => {
                // TODO: fill in impl later
                std.debug.print("FIRST TWO BITS 0b10 - DECODE LENGTH\n", .{});
                return StringVal{ .val = "test", .len = 0, .type = .string };
            },
            0b11 => {
                const last_six_bits = byte & 0b00111111;
                const len = @as(usize, @intCast(last_six_bits));

                switch (len) {
                    2 => {
                        const u32_val = std.mem.readInt(u32, &.{ self.next(), self.next(), self.next(), self.next() }, .little);

                        const to_str = try std.fmt.allocPrint(self.alloc, "{d}", .{u32_val});

                        return StringVal{ .val = to_str, .len = 2, .type = .integer };
                    },
                    1 => {
                        const u16_val = std.mem.readInt(u16, &.{ self.next(), self.next() }, .little);

                        const to_str = try std.fmt.allocPrint(self.alloc, "{d}", .{u16_val});

                        return StringVal{ .val = to_str, .len = 1, .type = .integer };
                    },
                    0 => {
                        const u8_val = std.mem.readInt(u8, &.{self.next()}, .little);

                        const to_str = try std.fmt.allocPrint(self.alloc, "{d}", .{u8_val});

                        return StringVal{ .val = to_str, .len = 0, .type = .integer };
                    },
                    else => {
                        return error.CannotDecodeTwoSignificantBits;
                    },
                }
            },
            else => {
                std.debug.print("DB SECTION - DUNNO \n", .{});
                return error.CannotDecodeTwoSignificantBits;
            },
        }
    }

    fn next(self: *RdbLoader) u8 {
        const curr_byte = self.bytes[self.index];

        self.index += 1;

        return curr_byte;
    }
};

test "keys with expiry rdb" {
    const alloc = std.testing.allocator;
    var rdb_loader = try RdbLoader.init(alloc, "dumps", "keys_with_expiry.rdb");
    defer rdb_loader.deinit(alloc);

    _ = try rdb_loader.parse();
}

test "int keys` rdb" {
    const alloc = std.testing.allocator;
    var rdb_loader = try RdbLoader.init(alloc, "dumps", "integer_keys.rdb");
    defer rdb_loader.deinit(alloc);

    _ = try rdb_loader.parse();
}
