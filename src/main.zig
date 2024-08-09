const std = @import("std");
const net = std.net;
const time = std.time;
const rand = std.crypto.random;
const Loc = struct { start: usize, end: usize };
const Tag = enum { echo, ping, set, get, info, replconf, psync };
const Command = struct { loc: Loc, tag: Tag, args: [2]Arg, opt: ?Arg = null };
const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };
const Role = enum { master, slave };

const Replica = struct {
    stream: net.Stream,

    pub fn init(stream: net.Stream) Replica {
        return .{ .stream = stream };
    }

    pub fn write(self: *Replica, cmd_buf: []const u8) !void {
        _ = try self.stream.write(cmd_buf);
    }
};

const ServerState = struct {
    replicas: []Replica,
    role: Role = .master,
    replica_count: u8 = 0,

    pub fn init() ServerState {
        return .{ .replicas = undefined };
    }

    pub fn forward_cmd(self: *ServerState, cmd_buf: []const u8) !void {
        if (self.replicas.len == 0) return error.NoReplicasToForwardCmd;

        try self.replicas[0].write(cmd_buf);
    }

    pub fn add_replica(self: *ServerState, stream: net.Stream) void {
        self.replicas[self.replica_count] = Replica.init(stream);

        self.replica_count += 1;
    }
};
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
                if (now < exp) {
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

    pub fn set(
        self: *RedisStore,
        key: []const u8,
        val: []const u8,
        exp: ?[]const u8,
    ) !void {
        var rv = RedisVal{ .val = val };
        if (exp) |e| {
            const now = time.milliTimestamp();
            const parse_to_int = try std.fmt.parseInt(i64, e, 10);

            rv.expiry = now + parse_to_int;
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

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "replconf")) |_| {
                command.tag = Tag.replconf;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "psync")) |_| {
                command.tag = Tag.psync;
            }
        }

        try self.expect_return_new_line_bytes();

        switch (command.tag) {
            Tag.ping => return command,
            Tag.echo, Tag.get => {
                command.args[0] = try self.parse_string();
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
            Tag.replconf => {
                while (std.ascii.isASCII(self.peek()) and self.peek() != 0) {
                    _ = try self.parse_string();

                    try self.expect_return_new_line_bytes();
                }

                return command;
            },
            Tag.psync => {
                if (self.peek() == '$') {
                    self.next();
                }

                while (std.ascii.isDigit(self.peek())) {
                    self.next();
                }

                try self.expect_return_new_line_bytes();

                var arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };
                if (self.peek() == '?') {
                    self.next();

                    arg.content = "?";
                    command.args[0] = arg;
                }

                try self.expect_return_new_line_bytes();

                if (self.peek() == '$') {
                    self.next();
                }

                while (std.ascii.isDigit(self.peek())) {
                    self.next();
                }

                try self.expect_return_new_line_bytes();

                var arg_two = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };
                if (self.peek() == '-') {
                    self.next();
                    if (self.peek() == '1') {
                        self.next();
                        arg_two.content = "0";

                        command.args[1] = arg_two;
                    }
                }
                try self.expect_return_new_line_bytes();

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

                    if (self.peek() == '-') {
                        self.next();
                    }

                    continue;
                } else {
                    break;
                }
            }

            arg.content = self.buffer[arg.loc.start .. arg.loc.end + 1];

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

test "test PSYNC" {
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    var parser = Parser.init(bytes);
    const command = try parser.parse();
    try std.testing.expectEqual(Tag.psync, command.tag);
    try std.testing.expectEqualSlices(u8, "?", command.args[0].content);
    try std.testing.expectEqualSlices(u8, "0", command.args[1].content);
}

test "test REPLCONF" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n";
    var parser = Parser.init(bytes);
    const command = try parser.parse();
    try std.testing.expectEqual(Tag.replconf, command.tag);
}

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

fn handle_echo(stream: net.Stream, arg: Arg) !void {
    const terminator = "\r\n";
    const length = arg.content.len;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, arg.content, terminator });
    defer allocator.free(resp);

    _ = try stream.write(resp);
}

fn handle_info(
    stream: net.Stream,
    is_replica: bool,
    master_replication_id: []u8,
) !void {
    const terminator = "\r\n";
    const val = if (is_replica) "role:slave" else "role:master\r\nmaster_repl_offset:0";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const replica_id_key_val = try std.fmt.allocPrint(allocator, "master_replid:{s}{s}", .{ master_replication_id, terminator });
    defer allocator.free(replica_id_key_val);

    const total_len = val.len + replica_id_key_val.len;

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}{s}", .{ total_len, terminator, val, terminator, replica_id_key_val });
    defer allocator.free(resp);

    _ = try stream.write(resp);
}

fn handle_ping(stream: net.Stream) !void {
    try stream.writeAll("+PONG\r\n");
}
fn handle_set(
    stream: net.Stream,
    store: *RedisStore,
    key: Arg,
    val: Arg,
    opt: ?Arg,
) !void {
    if (opt != null) {
        try store.set(key.content, val.content, opt.?.content);
    } else {
        try store.set(key.content, val.content, null);
    }

    const resp = "+OK\r\n";
    _ = try stream.write(resp);

    // const cwd = std.fs.cwd();
    // try cwd.writeFile2(.{ .sub_path = "db.rdb", .data = "test\r\nyoyoyo" });
    //
    // var read_buf: [40]u8 = undefined;
    //
    // const file = try cwd.openFile("db.rdb", .{});
    // defer file.close();
    //
    // var buf_reader = std.io.bufferedReader(file.reader());

    //
    // const num_bytes_read = try reader.read(&read_buf);
    //
    // std.debug.print("Buffer read: {any}, num_bytes_read: {any}\n", .{
    //     read_buf,
    //     num_bytes_read,
    // });
}
fn handle_get(stream: net.Stream, store: *RedisStore, key: Arg) !void {
    const val = store.get(key.content) catch |err| switch (err) {
        error.KeyHasExceededExpirationThreshold => {
            _ = try stream.write("$-1\r\n");

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

    _ = try stream.write(resp);
}

fn handle_replconf(stream: net.Stream) !void {
    try stream.writeAll("+OK\r\n");
}

fn handle_psync(
    allocator: std.mem.Allocator,
    stream: net.Stream,
    replication_master_id: []u8,
    args: []const Arg,
) !void {
    const resp = try std.fmt.allocPrint(
        allocator,
        "+FULLRESYNC {s} {s}\r\n",
        .{ replication_master_id, args[1].content },
    );
    defer allocator.free(resp);

    _ = try stream.write(resp);

    // const cwd = std.fs.cwd();
    // try cwd.writeFile2(.{ .sub_path = "db.rdb", .data = "test\r\nyoyoyo" });
    //
    // var read_buf: [40]u8 = undefined;
    //
    // const file = try cwd.openFile("db.rdb", .{});
    // defer file.close();
    //
    // var buf_reader = std.io.bufferedReader(file.reader());
    // const reader = buf_reader.reader();
    //
    // const num_bytes_read = try reader.read(&read_buf);
    //
    // std.debug.print("Buffer read: {any}, num_bytes_read: {any}\n", .{
    //     read_buf,
    //     num_bytes_read,
    // });

    const encoded_empty_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    const Decoder = std.base64.standard.Decoder;

    const decoded_length = try Decoder.calcSizeForSlice(encoded_empty_rdb);
    const decoded_buffer = try allocator.alloc(u8, decoded_length);
    defer allocator.free(decoded_buffer);

    try Decoder.decode(decoded_buffer, encoded_empty_rdb);

    const rdb_resp = try std.fmt.allocPrint(
        allocator,
        "${d}\r\n{s}",
        .{ decoded_length, decoded_buffer },
    );
    std.debug.print("Decoded length: {d}, decoded buffer: {any}\n", .{
        decoded_length,
        decoded_buffer,
    });
    defer allocator.free(rdb_resp);
    _ = try stream.write(rdb_resp);
}

fn handle_connection(stream: net.Stream, stdout: anytype, is_replica: bool, state: *ServerState) !void {
    defer stream.close();

    var buffer: [1024:0]u8 = undefined;

    const reader = stream.reader();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    var master_replication_id: [40:0]u8 = undefined;
    var i: usize = 0;
    while (i < master_replication_id.len) {
        const rand_int = rand.int(u8);

        if (std.ascii.isAlphanumeric(rand_int)) {
            master_replication_id[i] = rand_int;
            i += 1;
        }
    }

    while (try reader.read(&buffer) > 0) {
        try stdout.print("Connection received, buffer being read into\n", .{});
        var parser = Parser{ .buffer = &buffer, .curr_index = 0 };

        const command = try parser.parse();

        const opt = command.opt orelse null;

        switch (command.tag) {
            Tag.echo => try handle_echo(stream, command.args[0]),
            Tag.ping => try handle_ping(stream),
            Tag.set => {
                try handle_set(
                    stream,
                    &store,
                    command.args[0],
                    command.args[1],
                    opt,
                );
                try state.forward_cmd(&buffer);
            },
            Tag.get => try handle_get(stream, &store, command.args[0]),
            Tag.info => try handle_info(
                stream,
                is_replica,
                &master_replication_id,
            ),
            Tag.replconf => try handle_replconf(stream),
            Tag.psync => {
                try handle_psync(
                    allocator,
                    stream,
                    &master_replication_id,
                    &command.args,
                );

                state.add_replica(stream);
            },
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var args = std.process.args();
    _ = args.skip();

    var port: u16 = 6379;
    var master_port: ?[]const u8 = null;
    var is_replica = false;

    var state = ServerState.init();
    while (args.next()) |arg| {
        if (std.ascii.eqlIgnoreCase(arg, "--port")) {
            if (args.next()) |p| {
                port = try std.fmt.parseInt(u16, p, 10);
            }
        }

        if (std.ascii.eqlIgnoreCase(arg, "--replicaof")) {
            is_replica = true;

            while (args.next()) |a| {
                var start: usize = 0;
                var end: usize = 0;
                var addr: []const u8 = undefined;

                for (a, 0..) |ch, idx| {
                    if (ch == ' ') {
                        addr = a[start..end];
                        if (a[idx + 1] != ' ') {
                            start = idx + 1;
                        }
                        break;
                    }
                    end += 1;
                }

                master_port = a[start..];
                state.role = .slave;
            }
        }
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    if (is_replica and master_port != null) {
        std.debug.print("IS REPLICA HERE", .{});
        const master_address = try net.Address.resolveIp("127.0.0.1", try std.fmt.parseInt(u16, master_port.?, 10));
        const replica_stream = try net.tcpConnectToAddress(master_address);
        defer replica_stream.close();

        var replica_writer = replica_stream.writer();
        const ping_resp = "*1\r\n$4\r\nPING\r\n";
        _ = try replica_writer.write(ping_resp);

        var buffer: [1024:0]u8 = undefined;
        _ = try replica_stream.read(&buffer); // master responds w/ +PONG

        const allocator = gpa.allocator();
        const resp = try std.fmt.allocPrint(allocator, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{d}\r\n", .{port});
        defer allocator.free(resp);

        _ = try replica_stream.writer().write(resp);

        _ = try replica_stream.read(&buffer); // master responds w/ +OK
        _ = try replica_stream.writer().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        _ = try replica_stream.read(&buffer); // master responds w/ +OK
        _ = try replica_stream.writer().write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
    }

    const allocator = gpa.allocator();

    var threads = std.ArrayList(std.Thread).init(allocator);
    defer threads.deinit();

    const cpus = try std.Thread.getCpuCount();
    try stdout.print("CPU core count {}\n", .{cpus});

    while (true) {
        for (0..cpus) |_| {
            const client_connection = try server.accept();
            try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

            try threads.append(try std.Thread.spawn(
                .{},
                handle_connection,
                .{ client_connection.stream, stdout, is_replica, &state },
            ));
        }

        for (threads.items) |thread| thread.detach();
    }
}
