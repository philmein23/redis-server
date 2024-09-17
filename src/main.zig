const net = std.net;
const std = @import("std");
const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const ServerState = @import("type.zig").ServerState;
const RedisStore = @import("store.zig").RedisStore;
const Parser = @import("parser.zig").Parser;
const Parser_ = @import("parser.zig").Parser_;
const Command_ = @import("parser.zig").Command_;
const mutex = std.Thread.Mutex;

fn sync_rdb_with_master() !void {
    const cwd = std.fs.cwd();
    try cwd.writeFile2(.{ .sub_path = "db.rdb", .data = "test\r\nyoyoyo" });

    var read_buf: [40]u8 = undefined;

    const file = try cwd.openFile("db.rdb", .{});
    defer file.close();

    var buf_reader = std.io.bufferedReader(file.reader());
    const reader = buf_reader.reader();

    const num_bytes_read = try reader.read(&read_buf);

    std.debug.print("Buffer read: {any}, num_bytes_read: {any}\n", .{
        read_buf,
        num_bytes_read,
    });
}

fn handle_info(
    stream: net.Stream,
    allocator: std.mem.Allocator,
    state: *ServerState,
) !void {
    const terminator = "\r\n";
    const val = if (state.role != .master) "role:slave" else "role:master\r\nmaster_repl_offset:0";

    if (state.replication_id) |rep_id| {
        const replica_id_key_val = try std.fmt.allocPrint(allocator, "master_replid:{s}{s}", .{ rep_id, terminator });
        defer allocator.free(replica_id_key_val);

        const total_len = val.len + replica_id_key_val.len;

        const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}{s}", .{ total_len, terminator, val, terminator, replica_id_key_val });
        defer allocator.free(resp);

        _ = try stream.write(resp);
    }
}

fn handle_get(
    stream: net.Stream,
    allocator: std.mem.Allocator,
    store: *RedisStore,
    key: []const u8,
) !void {
    const val = store.get(key) catch |err| switch (err) {
        error.KeyHasExceededExpirationThreshold => {
            _ = try stream.write("$-1\r\n");

            return;
        },
        else => |e| return e,
    };

    const terminator = "\r\n";
    const length = val.len;

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, val, terminator });
    defer allocator.free(resp);

    _ = try stream.write(resp);
}

fn handle_psync(
    allocator: std.mem.Allocator,
    stream: net.Stream,
    state: *ServerState,
) !void {
    if (state.replication_id) |rep_id| {
        const resp = try std.fmt.allocPrint(
            allocator,
            "+FULLRESYNC {s} 0\r\n",
            .{rep_id},
        );
        defer allocator.free(resp);
        _ = try stream.write(resp);
    }

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

    defer allocator.free(rdb_resp);
    _ = try stream.write(rdb_resp);
}

fn handle_wait(
    allocator: std.mem.Allocator,
    stream: net.Stream,
    state: *ServerState,
    cmd: *const Command_,
) !void {
    var now = std.time.milliTimestamp();
    const to_expire_at = now + cmd.wait.exp;

    const get_ack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    try state.forward_cmd_2(get_ack_cmd);

    var num_replicas_acked: usize = 0;
    while (now < to_expire_at) {
        num_replicas_acked = 0;
        var iter = state.replicas_2.valueIterator();
        while (iter.next()) |replica| {
            if (replica.*.offset >= state.offset) {
                num_replicas_acked += 1;
            }
        }
        if (num_replicas_acked >= cmd.wait.num_replicas_to_ack) {
            const resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{num_replicas_acked});
            defer allocator.free(resp);

            _ = try stream.write(resp);
            return;
        }

        // Add a small delay to allow 'now' timestamp to increment
        std.time.sleep(100 * std.time.ns_per_ms);

        now = std.time.milliTimestamp();
    }

    const resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{num_replicas_acked});
    defer allocator.free(resp);

    _ = try stream.write(resp);
}

fn handle_connection(
    stream: net.Stream,
    stdout: anytype,
    allocator: std.mem.Allocator,
    state: *ServerState,
    store: *RedisStore,
) !void {
    defer {
        stream.close();
        std.debug.print("Closing connection....", .{});
    }

    var buffer: [1024:0]u8 = undefined;
    const reader = stream.reader();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gpa.deinit(); // commmenting this out resolves gpa memory leak issue -- still trying to understand why
    // const allocator = gpa.allocator();
    //
    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    var get_ack_count: usize = 0;

    while (true) {
        std.debug.print("ABOUT TO READ - ROLE: {any}\n", .{state.role});
        const bytes_read = reader.read(&buffer) catch |err| switch (err) {
            error.ConnectionResetByPeer => {
                std.debug.print("CONNECTION RESET BY PEER ERROR - THREAD ID: {any}\n", .{std.Thread.getCurrentId()});

                return err;
            },
            else => |e| return e,
        };
        if (bytes_read == 0) break;

        try bytes.appendSlice(buffer[0..bytes_read]);
        const bytes_slice = try bytes.toOwnedSliceSentinel(0);

        try stdout.print("Connection received, buffer being read into...{s}\n", .{bytes_slice});
        var parser = try Parser_.init(allocator, bytes_slice);
        const cmds = try parser.parse_();
        defer allocator.free(cmds);

        for (cmds) |cmd| {
            switch (cmd) {
                .config => {
                    switch (cmd.config) {
                        .get => {
                            const terminator = "\r\n";
                            var buf: [100]u8 = undefined;
                            if (std.mem.eql(u8, cmd.config.get, "dir")) {
                                const resp = try std.fmt.bufPrint(&buf, "*2{s}$3{s}dir{s}${d}{s}{s}{s}", .{ terminator, terminator, terminator, state.dir.len, terminator, state.dir, terminator });

                                _ = try stream.write(resp);
                            }
                            if (std.mem.eql(u8, cmd.config.get, "dbfilename")) {}
                        },
                    }
                },
                .echo => {
                    const terminator = "\r\n";

                    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{
                        cmd.echo.len,
                        terminator,
                        cmd.echo,
                        terminator,
                    });
                    defer allocator.free(resp);

                    _ = try stream.write(resp);
                },
                .ping => {
                    switch (state.role) {
                        .master => {
                            _ = try stream.write("+PONG\r\n");
                        },
                        .slave => {
                            if (state.replicas_2.get(stream.handle)) |replica| {
                                replica.*.offset += bytes_slice.len;
                            }
                        },
                    }
                },
                .set => {
                    try store.set(cmd.set.key, cmd.set.val, cmd.set.px);

                    switch (state.role) {
                        .master => {
                            try state.forward_cmd_2(bytes_slice);
                            state.offset += bytes_slice.len;

                            _ = try stream.write("+OK\r\n");
                        },
                        .slave => {
                            if (state.replicas_2.get(stream.handle)) |replica| {
                                replica.*.offset += bytes_slice.len;
                            }
                        },
                    }
                },
                .get => try handle_get(
                    stream,
                    allocator,
                    store,
                    cmd.get.key,
                ),
                .info => try handle_info(
                    stream,
                    allocator,
                    state,
                ),
                .wait => {
                    try handle_wait(allocator, stream, state, &cmd);
                },
                .replconf => {
                    switch (cmd.replconf) {
                        .getack => {
                            switch (state.role) {
                                .master => {
                                    const get_ack_cmd = "*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
                                    try state.forward_cmd_2(get_ack_cmd);
                                },
                                .slave => {
                                    if (state.replicas_2.get(stream.handle)) |replica| {
                                        const digit_to_bytes = try std.fmt.allocPrint(allocator, "{d}", .{replica.*.offset});
                                        defer allocator.free(digit_to_bytes);

                                        const resp = try std.fmt.allocPrint(allocator, "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${d}\r\n{d}\r\n", .{ digit_to_bytes.len, replica.*.offset });
                                        defer allocator.free(resp);

                                        _ = try stream.write(resp);

                                        replica.*.offset += resp.len;

                                        get_ack_count += 1;
                                    }
                                },
                            }
                        },
                        .ack => {
                            if (state.replicas_2.get(stream.handle)) |replica| {
                                replica.*.offset += cmd.replconf.ack;
                            }
                        },
                        else => {
                            _ = try stream.write("+OK\r\n");
                        },
                    }
                },
                .psync => {
                    try handle_psync(
                        allocator,
                        stream,
                        state,
                    );

                    try state.add_replica_2(stream);
                },
            }
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var state = ServerState.init(allocator);
    defer state.deinit();

    try handle_args(allocator, &state);

    if (state.role == .master) {
        try state.generate_master_replication_id();
    }

    const address = try net.Address.resolveIp("127.0.0.1", state.port);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var store = RedisStore.init(allocator);
    defer store.deinit();

    // const T1 = struct { fd: [5]u8 };
    // const t = T1{ .fd = undefined };
    // std.debug.print("TEST ALIGNOF: {}\n", .{@alignOf(T1)});
    // std.debug.print("T1 Var address: {*}\n", .{&t});
    // std.debug.print("T1 fd field address: {*}\n", .{&t.fd});

    if (state.master_port != null) {
        const replica_stream = try handle_handshake(&state, allocator);

        const thread = try std.Thread.spawn(
            .{},
            handle_connection,
            .{
                replica_stream,
                stdout,
                allocator,
                &state,
                &store,
            },
        );
        thread.detach();
    }

    while (true) {
        const client_connection = try server.accept();
        try stdout.print("Connection received {} is sending data..\n", .{client_connection.address});

        const thread = try std.Thread.spawn(
            .{},
            handle_connection,
            .{
                client_connection.stream,
                stdout,
                allocator,
                &state,
                &store,
            },
        );
        thread.detach();
    }
}

fn handle_args(allocator: std.mem.Allocator, state: *ServerState) !void {
    var args = std.process.args();
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.ascii.eqlIgnoreCase(arg, "--dir")) {
            if (args.next()) |dir_name| {
                if (std.ascii.eqlIgnoreCase(args.next().?, "--dbfilename")) {
                    if (args.next()) |filename| {
                        const cwd = std.fs.cwd();

                        try cwd.makePath(dir_name);

                        const file = try cwd.createFile(try std.fs.path.join(allocator, &[_][]const u8{ dir_name, filename }), .{});
                        defer file.close();

                        state.dir = dir_name;
                        state.dbfilename = filename;
                    }
                }
            }
        }

        if (std.ascii.eqlIgnoreCase(arg, "--port")) {
            if (args.next()) |p| {
                state.port = try std.fmt.parseInt(u16, p, 10);
            }
        }

        if (std.ascii.eqlIgnoreCase(arg, "--replicaof")) {
            if (args.next()) |master_host_port| {
                var start: usize = 0;
                var end: usize = 0;

                for (master_host_port, 0..) |ch, idx| {
                    if (ch == ' ') {
                        state.master_host = master_host_port[start..end];
                        if (master_host_port[idx + 1] != ' ') {
                            start = idx + 1;
                        }
                        break;
                    }
                    end += 1;
                }

                state.master_port = try std.fmt.parseInt(u16, master_host_port[start..], 10);
                if (std.ascii.eqlIgnoreCase(state.master_host.?, "localhost")) {
                    state.master_host = "127.0.0.1";
                }
            }

            state.role = .slave;
        }
    }
}

fn handle_handshake(state: *ServerState, allocator: std.mem.Allocator) !std.net.Stream {
    const master_address = try net.Address.resolveIp(state.master_host.?, state.master_port.?);
    const replica_stream = try net.tcpConnectToAddress(master_address);

    var replica_writer = replica_stream.writer();
    const ping_resp = "*1\r\n$4\r\nPING\r\n";

    _ = try replica_writer.write(ping_resp);

    var buffer: [1024:0]u8 = undefined;
    _ = try replica_stream.read(&buffer); // master responds w/ +PONG

    const resp = try std.fmt.allocPrint(allocator, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{d}\r\n", .{state.port});
    defer allocator.free(resp);

    _ = try replica_stream.writer().write(resp);
    _ = try replica_stream.read(&buffer); // master responds w/ +OK
    _ = try replica_stream.writer().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
    _ = try replica_stream.read(&buffer); // master responds w/ +OK
    _ = try replica_stream.writer().write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");

    var buf2: [56]u8 = undefined;
    const bytes_read = try replica_stream.readAll(&buf2); // reads FULLSYNC response from master
    var bound: usize = 0;

    for (buf2, 0..) |ch, i| {
        if (ch == ' ') {
            bound = i + 1;
            break;
        }
    }

    const buffer_two = buf2[bound..bytes_read];

    for (buffer_two, 0..) |ch, i| {
        if (ch == ' ') {
            bound = i - 1;
            break;
        }
    }

    const rep_id = buffer_two[0..bound];

    state.replication_id = rep_id;

    var buf3: [93]u8 = undefined;
    _ = try replica_stream.readAll(&buf3); // reads empty RDB file from master

    return replica_stream;
}
