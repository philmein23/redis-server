const std = @import("std");
const net = std.net;
const time = std.time;
const rand = std.crypto.random;

const Loc = struct { start: usize, end: usize };
const Tag = enum { echo, ping, set, get, info };
const Command = struct { loc: Loc, tag: Tag, args: [2]Arg, opt: ?Arg = null };
const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };

const RedisStore = struct {
    table: std.StringHashMap(RedisVal),

    const RedisVal = struct {
        val: []const u8,
        expiry: ?i64 = null,
    };

    pub fn init(alloc: std.mem.Allocator) RedisStore {
        return RedisStore{ .table = std.StringHashMap(RedisVal).init(alloc) };
    }

    pub fn deinit(self: *RedisStore) void {
        self.table.deinit();
    }

    pub fn get(self: *RedisStore, key: []const u8) ![]const u8 {
        if (self.table.get(key)) |v| {
            if (v.expiry) |exp| {
                const now = time.milliTimestamp();
                if (now <= exp) {
                    return v.val;
                } else {
                    return error.KeyHasExceededExpirationThreshold;
                }
            }
            return v.val;
        } else {
            return error.NoValueExistforGivenKey;
        }
    }

    pub fn set(self: *RedisStore, key: []const u8, val: []const u8, exp: ?[]const u8) !void {
        var rv = RedisVal{ .val = val };
        if (exp) |e| {
            const now = time.milliTimestamp();
            const parse_to_int = try std.fmt.parseInt(i64, e, 10);

            rv.expiry = now + parse_to_int;
            std.debug.print("RedisStore SET - KEY: {s}, VAL: {s}, NOW: {any}, EXP: {any}, NEW_EXP: {any}\n", .{ key, val, now, parse_to_int, rv.expiry });
        }
        try self.table.put(key, rv);
    }
};

const Parser = struct {
    buffer: [:0]const u8,
    curr_index: usize,

    pub fn init(buffer: [:0]const u8) Parser {
        return Parser{ .buffer = buffer, .curr_index = 0 };
    }

    pub fn parse(self: *Parser) !Command {
        var command = Command{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .args = undefined };
        // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"
        if (self.peek() == '*') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
            self.next();
        }
        try self.expect_return_new_line_bytes();

        if (self.peek() == '$') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        try self.expect_return_new_line_bytes();

        // parse command
        if (std.ascii.isAlphabetic(self.peek())) {
            self.next();
            command.loc.start = self.curr_index;

            while (true) {
                if (std.ascii.isAlphabetic(self.peek())) {
                    self.next();
                    command.loc.end = self.curr_index;

                    continue;
                } else {
                    break;
                }
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "echo")) |_| {
                command.tag = Tag.echo;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "ping")) |_| {
                command.tag = Tag.ping;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "set")) |_| {
                command.tag = Tag.set;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "get")) |_| {
                command.tag = Tag.get;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "info")) |_| {
                command.tag = Tag.info;
            }
        }

        try self.expect_return_new_line_bytes();

        switch (command.tag) {
            Tag.ping => return command,
            Tag.echo, Tag.get => {
                command.args[0] = try self.parse_string();
                std.debug.print("Command.args[0] address: {*}, {*}\n", .{ &command.args[0], &command.args });
                std.debug.print("Arg address (after returned): {*}, {*}\n", .{ &command.args[0], &command.args[0].loc });
                return command;
            },
            Tag.set => {
                command.args[0] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                command.args[1] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                // try to parse optional expiry parameter
                if (self.peek() != '$') return command;

                self.next();

                const px = try self.parse_string();

                if (std.ascii.indexOfIgnoreCase(self.buffer[px.loc.start .. px.loc.end + 1], "px")) |_| {
                    try self.expect_return_new_line_bytes();

                    command.opt = try self.parse_string();
                }
                try self.expect_return_new_line_bytes();

                return command;
            },
            Tag.info => {
                command.args[0] = try self.parse_string();

                return command;
            },
        }
    }

    fn parse_string(self: *Parser) !Arg {
        if (self.peek() == '$') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        try self.expect_return_new_line_bytes();

        if (std.ascii.isAlphanumeric(self.peek())) {
            self.next();
            var arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };

            arg.loc.start = self.curr_index;

            while (true) {
                if (std.ascii.isAlphanumeric(self.peek())) {
                    self.next();
                    arg.loc.end = self.curr_index;

                    continue;
                } else {
                    break;
                }
            }

            std.debug.print("Command ARG content {s}\n", .{self.buffer[arg.loc.start .. arg.loc.end + 1]});

            arg.content = self.buffer[arg.loc.start .. arg.loc.end + 1];

            std.debug.print("Arg address (Before returned): {*}, {*}\n", .{ &arg, &arg.loc });

            return arg;
        } else {
            return error.BytesAreNotAlphanumeric;
        }
    }
    fn next(self: *Parser) void {
        self.curr_index += 1;
    }
    fn peek(self: *Parser) u8 {
        return self.buffer[self.curr_index + 1];
    }

    fn expect_return_new_line_bytes(self: *Parser) !void {
        if (self.peek() == '\r') {
            self.next();
        } else {
            return error.ExpectedCarriageReturnByte;
        }

        if (self.peek() == '\n') {
            self.next();
        } else {
            return error.ExpectedNewLineByte;
        }
    }
};

test "test SET with expiry opt" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    var parser = Parser.init(bytes);
    const command = try parser.parse();
    std.debug.print("Command key-val content- key: {s}, val: {s}\n", .{ command.args[1].content, command.args[1].content });
    try store.set(command.args[0].content, command.args[1].content, command.opt.?.content);
    try std.testing.expectEqual(Tag.set, command.tag);
    try std.testing.expectEqualSlices(u8, "100", command.opt.?.content);
}

test "test INFO command" {
    const bytes = "*3\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    var parser = Parser.init(bytes);
    const command = try parser.parse();

    try std.testing.expectEqual(Tag.info, command.tag);
    try std.testing.expectEqualSlices(u8, "replication", command.args[0].content);
}

test "test SET and GET command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    var parser = Parser.init(bytes);
    const command = try parser.parse();
    std.debug.print("Command key-val content- key: {s}, val: {s}\n", .{ command.args[0].content, command.args[1].content });
    try store.set(command.args[0].content, command.args[1].content, null);
    try std.testing.expectEqual(Tag.set, command.tag);

    const bytes_two = "*3\r\n$3\r\nGET\r\n$5\r\napple\r\n";
    var parser_two = Parser.init(bytes_two);
    const command_two = try parser_two.parse();
    const get_value = try store.get(command_two.args[0].content);

    try std.testing.expectEqual(Tag.get, command_two.tag);
    try std.testing.expectEqualSlices(u8, "pear", get_value);
}

test "test parse PING command" {
    const bytes = "*1\r\n$4\r\nping\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var parser = Parser.init(bytes);
    const command = try parser.parse();

    std.debug.print("Command ARG content: {s}\n", .{command.args[0].content});
    try std.testing.expectEqual(Tag.ping, command.tag);
}

test "test parse ECHO command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var parser = Parser.init(bytes);
    const command = try parser.parse();

    try std.testing.expectEqual(Tag.echo, command.tag);
    const exp = "pineapple";
    try std.testing.expectEqualSlices(u8, exp, command.args[0].content);
}

fn handle_echo(client_connection: net.Server.Connection, arg: Arg) !void {
    const terminator = "\r\n";
    const length = arg.content.len;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, arg.content, terminator });
    defer allocator.free(resp);

    _ = try client_connection.stream.writeAll(resp);
}

fn handle_info(client_connection: net.Server.Connection, is_replica: bool) !void {
    const terminator = "\r\n";
    const val = if (is_replica) "role:slave" else "role:master\r\nmaster_repl_offset:0";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var random_int_buffer: [40:0]u8 = undefined;

    var i: usize = 0;
    while (i < random_int_buffer.len) {
        const rand_int = rand.int(u8);

        if (std.ascii.isAlphanumeric(rand_int)) {
            random_int_buffer[i] = rand_int;
            i += 1;
        }
    }

    const replica_id_key_val = try std.fmt.allocPrint(allocator, "master_replid:{s}", .{random_int_buffer});
    defer allocator.free(replica_id_key_val);

    const total_len = val.len + replica_id_key_val.len;

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}{s}{s}", .{ total_len, terminator, val, terminator, replica_id_key_val, terminator });
    std.log.info("REPL_ID LOG - {s}\n", .{resp});
    defer allocator.free(resp);

    _ = try client_connection.stream.writeAll(resp);
}

fn handle_ping(client_connection: net.Server.Connection) !void {
    try client_connection.stream.writeAll("+PONG\r\n");
}
fn handle_set(client_connection: net.Server.Connection, store: *RedisStore, key: Arg, val: Arg, opt: ?Arg) !void {
    if (opt != null) {
        try store.set(key.content, val.content, opt.?.content);
    } else {
        try store.set(key.content, val.content, null);
    }

    const resp = "+OK\r\n";
    _ = try client_connection.stream.writeAll(resp);
}
fn handle_get(client_connection: net.Server.Connection, store: *RedisStore, key: Arg) !void {
    const val = store.get(key.content) catch |err| switch (err) {
        error.KeyHasExceededExpirationThreshold => {
            try client_connection.stream.writeAll("$-1\r\n");

            return;
        },
        else => |e| return e,
    };

    const terminator = "\r\n";
    const length = val.len;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, val, terminator });
    defer allocator.free(resp);

    _ = try client_connection.stream.writeAll(resp);
}

fn handle_connection(client_connection: net.Server.Connection, stdout: anytype, is_replica: bool) !void {
    defer client_connection.stream.close();

    var buffer: [1024:0]u8 = undefined;

    const reader = client_connection.stream.reader();

    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    while (try reader.read(&buffer) > 0) {
        var parser = Parser{ .buffer = &buffer, .curr_index = 0 };

        const command = try parser.parse();

        const opt = command.opt orelse null;

        switch (command.tag) {
            Tag.echo => try handle_echo(client_connection, command.args[0]),
            Tag.ping => try handle_ping(client_connection),
            Tag.set => try handle_set(client_connection, &store, command.args[0], command.args[1], opt),
            Tag.get => try handle_get(client_connection, &store, command.args[0]),
            Tag.info => try handle_info(client_connection, is_replica),
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var args = std.process.args();
    _ = args.skip();

    var port: u16 = 6379;
    var is_replica = false;
    while (args.next()) |arg| {
        if (std.ascii.eqlIgnoreCase(arg, "--port")) {
            if (args.next()) |p| {
                port = try std.fmt.parseInt(u16, p, 10);
            }
        }

        if (std.ascii.eqlIgnoreCase(arg, "--replicaof")) {
            is_replica = true;
        }
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var threads = std.ArrayList(std.Thread).init(allocator);
    defer threads.deinit();

    const cpus = try std.Thread.getCpuCount();
    try stdout.print("CPU core count {}\n", .{cpus});

    while (true) {
        for (0..cpus) |_| {
            const client_connection = try server.accept();

            try threads.append(try std.Thread.spawn(.{}, handle_connection, .{ client_connection, stdout, is_replica }));
        }

        for (threads.items) |thread| thread.detach();
    }
}
