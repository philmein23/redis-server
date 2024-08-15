const net = std.net;
const rand = std.crypto.random;
const std = @import("std");
const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const ServerState = @import("type.zig").ServerState;
const RedisStore = @import("store.zig").RedisStore;
const Parser = @import("parser.zig").Parser;
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
    key: Arg,
) !void {
    const val = store.get(key.content) catch |err| switch (err) {
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
    args: []Arg,
) !void {
    if (state.replication_id) |rep_id| {
        const resp = try std.fmt.allocPrint(
            allocator,
            "+FULLRESYNC {s} {s}\r\n",
            .{ rep_id, args[1].content },
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
    std.debug.print("Decoded length: {d}, decoded buffer: {any}\n", .{
        decoded_length,
        decoded_buffer,
    });
    defer allocator.free(rdb_resp);
    _ = try stream.write(rdb_resp);
}

fn handle_connection(
    stream: net.Stream,
    stdout: anytype,
    allocator: std.mem.Allocator,
    state: *ServerState,
    store: *RedisStore,
) !void {
    var close_stream = true;
    // var mtx: std.Thread.Mutex = .{};
    // mtx.lock();

    defer {
        // mtx.unlock();
        if (close_stream) {
            stream.close();
            std.debug.print("Closing connection....", .{});
        }
    }

    var buffer: [100:0]u8 = undefined;
    const reader = stream.reader();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gpa.deinit(); // commmenting this out resolves gpa memory leak issue -- still trying to understand why
    // const allocator = gpa.allocator();
    //
    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    while (true) {
        const bytes_read = try reader.read(&buffer);

        if (bytes_read == 0) break;

        try bytes.appendSlice(buffer[0..bytes_read]);
        const bytes_slice = try bytes.toOwnedSliceSentinel(0);

        std.debug.print(
            "COMMANDS:{s}\n",
            .{bytes_slice},
        );

        try stdout.print("Connection received, buffer being read into...\n", .{});
        var parser = Parser.init(allocator, bytes_slice);
        var cmds = try parser._parse();
        defer cmds.deinit();

        for (cmds) |cmd| {
            const opt = cmd.opt orelse null;

            switch (cmd.tag) {
                Tag.echo => {
                    const echo_arg = cmd.args[0].content;
                    const terminator = "\r\n";
                    const length = echo_arg.len;

                    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{
                        length,
                        terminator,
                        echo_arg,
                        terminator,
                    });
                    defer allocator.free(resp);

                    _ = try stream.write(resp);
                },
                Tag.ping => {
                    _ = try stream.write("+PONG\r\n");
                },
                Tag.set => {
                    if (opt != null) {
                        try store.set(cmd.args[0].content, cmd.args[1].content, opt.?.content);
                    } else {
                        try store.set(cmd.args[0].content, cmd.args[1].content, null);
                    }

                    if (state.role == .master) {
                        _ = try stream.write("+OK\r\n");
                        try state.forward_cmd(bytes_slice);
                    }
                },
                Tag.get => try handle_get(
                    stream,
                    allocator,
                    store,
                    cmd.args[0],
                ),
                Tag.info => try handle_info(
                    stream,
                    allocator,
                    state,
                ),
                Tag.replconf => {
                    close_stream = false;
                    _ = try stream.write("+OK\r\n");
                },
                Tag.psync => {
                    try handle_psync(
                        allocator,
                        stream,
                        state,
                        &cmd.args,
                    );

                    state.add_replica(stream);
                },
            }
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var args = std.process.args();
    _ = args.skip();

    var port: u16 = 6379;

    var state = ServerState.init();

    while (args.next()) |arg| {
        if (std.ascii.eqlIgnoreCase(arg, "--port")) {
            if (args.next()) |p| {
                port = try std.fmt.parseInt(u16, p, 10);
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

    if (state.role == .master) {
        var master_replication_id: [40:0]u8 = undefined;
        var i: usize = 0;
        while (i < master_replication_id.len) {
            const rand_int = rand.int(u8);

            if (std.ascii.isAlphanumeric(rand_int)) {
                master_replication_id[i] = rand_int;
                i += 1;
            }
        }
        state.replication_id = &master_replication_id;
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var store = RedisStore.init(allocator);
    defer store.deinit();

    if (state.master_port != null) {
        const master_address = try net.Address.resolveIp(state.master_host.?, state.master_port.?);
        const replica_stream = try net.tcpConnectToAddress(master_address);

        var replica_writer = replica_stream.writer();
        const ping_resp = "*1\r\n$4\r\nPING\r\n";
        _ = try replica_writer.write(ping_resp);

        var buffer: [1024:0]u8 = undefined;
        _ = try replica_stream.read(&buffer); // master responds w/ +PONG

        const resp = try std.fmt.allocPrint(allocator, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{d}\r\n", .{port});
        defer allocator.free(resp);

        _ = try replica_stream.writer().write(resp);
        _ = try replica_stream.read(&buffer); // master responds w/ +OK
        _ = try replica_stream.writer().write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        _ = try replica_stream.read(&buffer); // master responds w/ +OK
        _ = try replica_stream.writer().write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        const bytes_read = try replica_stream.read(&buffer); // reads FULLSYNC response from master
        var bound: usize = 0;

        for (buffer, 0..) |ch, i| {
            if (ch == ' ') {
                bound = i + 1;
                break;
            }
        }

        const buffer_two = buffer[bound..bytes_read];

        for (buffer_two, 0..) |ch, i| {
            if (ch == ' ') {
                bound = i - 1;
                break;
            }
        }

        const rep_id = buffer_two[0..bound];

        state.replication_id = rep_id;

        const rdb_bytes_read = try replica_stream.read(&buffer); // reads empty RDB file from master
        std.debug.print("RDB Bytes read {}\n", .{rdb_bytes_read});

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
        thread.join();
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
