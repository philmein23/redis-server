const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const RedisStore = @import("store.zig").RedisStore;
const std = @import("std");

pub const Command_ = union(enum) {
    ping,
    info: Info,
    set: Set,
    get: Get,
    psync: Psync,
    replconf: Replconf,
    wait: Wait,
    echo: Echo,
    config: Config,

    const Config = union(enum) { get: []const u8 };

    const Echo = []const u8;

    const Wait = struct { num_replicas_to_ack: usize, exp: i64 };

    const Info = enum { replication };

    const Set = struct {
        key: []const u8,
        val: []const u8,
        px: ?i64 = null,
    };
    const Get = struct {
        key: []const u8,
    };

    const Replconf = union(enum) {
        listening_port,
        capa_psync2,
        ack: usize,
        getack: []const u8,
    };

    const Psync = struct { replication_id: []const u8, offset: isize };
};

pub const Token = struct {
    loc: Loc2,
    tag: Tag2,
    pub const Loc2 = struct {
        start: usize,
        end: usize,
    };
    pub const Tag2 = enum { dollar, asterisk, colon, number_literal, string_literal, question_mark, minus, carriage_return, eoc };
};

pub const Tokenizer = struct {
    buffer: [:0]const u8,
    index: usize = 0,

    const State = enum { start, int, string_literal, minus, carriage };

    pub fn init(source: [:0]const u8) Tokenizer {
        return .{ .buffer = source };
    }

    pub fn next(self: *Tokenizer) Token {
        var state: State = .start;
        var result: Token = .{
            .tag = undefined,
            .loc = .{
                .start = self.index,
                .end = undefined,
            },
        };

        while (true) : (self.index += 1) {
            const c = self.buffer[self.index];

            switch (state) {
                .start => {
                    switch (c) {
                        '\r' => {
                            state = .carriage;
                        },
                        'a'...'z', 'A'...'Z', '_' => {
                            state = .string_literal;
                            result.tag = .string_literal;
                        },
                        '0'...'9' => {
                            state = .int;
                            result.tag = .number_literal;
                        },
                        '-' => {
                            state = .minus;
                            result.tag = .minus;
                        },
                        '?' => {
                            result.tag = .question_mark;
                            self.index += 1;
                            break;
                        },
                        '*' => {
                            result.tag = .asterisk;
                            self.index += 1;
                            break;
                        },
                        '$' => {
                            result.tag = .dollar;
                            self.index += 1;
                            break;
                        },
                        ':' => {
                            result.tag = .colon;
                            self.index += 1;
                            break;
                        },
                        else => {
                            result.tag = .eoc;
                            break;
                        },
                    }
                },
                .string_literal => switch (c) {
                    'a'...'z', 'A'...'Z', '_', '-' => {
                        continue;
                    },
                    else => {
                        break;
                    },
                },
                .minus => switch (c) {
                    '1' => {
                        result.tag = .number_literal;
                        self.index += 1;
                        break;
                    },
                    else => {
                        break;
                    },
                },
                .int => switch (c) {
                    '0'...'9' => {
                        continue;
                    },
                    else => {
                        break;
                    },
                },
                .carriage => switch (c) {
                    '\n' => {
                        result.tag = .carriage_return;
                        self.index += 1;
                        break;
                    },
                    else => {
                        break;
                    },
                },
            }
        }

        result.loc.end = self.index;
        return result;
    }
};

test "echo tokenizer" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .eoc,
    });
}

test "set tokenizer" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .eoc,
    });
}

test "psync tokenizer" {
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .question_mark,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .number_literal,
        .carriage_return,
        .eoc,
    });
}

fn testTokenize(source: [:0]const u8, expected_token_tags: []const Token.Tag2) !void {
    var tokenizer = Tokenizer.init(source);
    for (expected_token_tags) |expected_token_tag| {
        const token = tokenizer.next();
        try std.testing.expectEqual(expected_token_tag, token.tag);
    }

    const last_token = tokenizer.next();
    try std.testing.expectEqual(source.len, last_token.loc.start);
    try std.testing.expectEqual(source.len, last_token.loc.end);
}

pub const Parser_ = struct {
    source: [:0]const u8,
    commands: std.ArrayList(Command_),
    cmd_offset: usize = 0,

    pub fn init(gpa: std.mem.Allocator, source: [:0]const u8) !Parser_ {
        const cmds = std.ArrayList(Command_).init(gpa);

        return .{ .source = source, .commands = cmds };
    }

    pub fn deinit(self: *Parser_) void {
        self.commands.deinit();
    }

    pub fn parse_(self: *Parser_) ![]Command_ {
        var cmd_iter = std.mem.splitSequence(u8, self.source, "\r\n");

        while (true) {
            // std.debug.print("BEGIN PARSE - CMD ITER:\nCURR INDEX: {d}\n CURR VAL: {d}\n", .{ cmd_iter.index.?, self.source[cmd_iter.index.?] });
            const start = cmd_iter.index.?;
            if (cmd_iter.peek() == null or cmd_iter.peek().?.len == 0) {
                _ = cmd_iter.next();

                break;
            }
            if (cmd_iter.peek()) |maybe_type| {
                const part = maybe_type[0];

                switch (part) {
                    '*' => {
                        _ = cmd_iter.next(); // consume RESP type
                        _ = cmd_iter.next(); // consume cmd length token

                        const cmd_string = cmd_iter.next().?; // consume cmd string

                        if (std.ascii.eqlIgnoreCase(cmd_string, "config")) {
                            _ = cmd_iter.next(); // consume length token
                            const subcommand = cmd_iter.next().?; // consume subcommand

                            if (std.ascii.eqlIgnoreCase(subcommand, "get")) {
                                _ = cmd_iter.next(); // consume length token
                                const config_param = cmd_iter.next().?; // consume the config value

                                try self.commands.append(Command_{ .config = .{ .get = config_param } });
                                continue;
                            }
                        }
                        if (std.ascii.eqlIgnoreCase(cmd_string, "psync")) {
                            _ = cmd_iter.next(); // consume psync rep id length token

                            const rep_id = cmd_iter.next().?; // consume psync rep id token

                            _ = cmd_iter.next(); // consume wait psync offset length token

                            const offset = try std.fmt.parseInt(isize, cmd_iter.next().?, 10); // consume psync offset val

                            try self.commands.append(
                                Command_{ .psync = .{
                                    .replication_id = rep_id,
                                    .offset = offset,
                                } },
                            );
                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "wait")) {
                            _ = cmd_iter.next(); // consume set key length token

                            const num = try std.fmt.parseInt(usize, cmd_iter.next().?, 10); // consume num rep ack val

                            _ = cmd_iter.next(); // consume wait val length token
                            const exp = try std.fmt.parseInt(i64, cmd_iter.next().?, 10); // consume wait val

                            try self.commands.append(Command_{ .wait = .{
                                .num_replicas_to_ack = num,
                                .exp = exp,
                            } });
                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "set")) {
                            _ = cmd_iter.next(); // consume set key length token

                            const key = cmd_iter.next().?; // consume key val
                            _ = cmd_iter.next(); // consume set val length token
                            const val = cmd_iter.next().?; // consume val

                            var cmd = Command_{ .set = .{
                                .key = key,
                                .val = val,
                            } };

                            const maybe_asterisk = std.mem.indexOf(u8, cmd_iter.peek().?, "*");
                            if (maybe_asterisk == null and cmd_iter.peek().?.len > 0) {
                                _ = cmd_iter.next(); // consume set some len token

                                if (std.ascii.eqlIgnoreCase(cmd_iter.peek().?, "px")) {
                                    _ = cmd_iter.next(); // consume set px token
                                    _ = cmd_iter.next(); // consume set px val len token
                                    cmd.set.px = try std.fmt.parseInt(i64, cmd_iter.next().?, 10); // consume px val token
                                }
                            }
                            try self.commands.append(cmd);

                            self.cmd_offset += self.source[start..cmd_iter.index.?].len;

                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "get")) {
                            _ = cmd_iter.next(); // consume set key length token
                            const key = cmd_iter.next().?; // consume key val

                            try self.commands.append(Command_{ .get = .{
                                .key = key,
                            } });
                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "echo")) {
                            _ = cmd_iter.next(); // consume set key length token
                            const val = cmd_iter.next().?; // consume val
                            try self.commands.append(Command_{ .echo = val });

                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "ping")) {
                            try self.commands.append(Command_{ .ping = {} });

                            self.cmd_offset += self.source[start..cmd_iter.index.?].len;

                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "info")) {
                            _ = cmd_iter.next(); // consume length token
                            _ = cmd_iter.next(); // consume replication val
                            try self.commands.append(Command_{ .info = .replication });
                            continue;
                        }

                        if (std.ascii.eqlIgnoreCase(cmd_string, "replconf")) {
                            _ = cmd_iter.next(); // consume length token
                            const subcommand = cmd_iter.next().?; // consume subcommand

                            if (std.ascii.eqlIgnoreCase(subcommand, "listening-port")) {
                                try self.commands.append(Command_{ .replconf = .{ .listening_port = {} } });

                                _ = cmd_iter.next(); // consume port len - don't need it
                                _ = cmd_iter.next(); // consume port - don't need it
                                continue;
                            } else if (std.ascii.eqlIgnoreCase(subcommand, "getack")) {
                                _ = cmd_iter.next(); // consume length token
                                const getack_asterisk = cmd_iter.next().?; // consume asterisk

                                try self.commands.append(Command_{ .replconf = .{ .getack = getack_asterisk } });
                                self.cmd_offset += self.source[start..cmd_iter.index.?].len;

                                continue;
                            } else if (std.ascii.eqlIgnoreCase(subcommand, "ack")) {
                                _ = cmd_iter.next(); // consume length token
                                const ack_val = try std.fmt.parseInt(usize, cmd_iter.next().?, 10); // consume ack val

                                try self.commands.append(Command_{ .replconf = .{ .ack = ack_val } });
                                continue;
                            } else if (std.ascii.eqlIgnoreCase(subcommand, "capa")) {
                                _ = cmd_iter.next(); // consume length token
                                _ = cmd_iter.next(); // consume psync2 token

                                try self.commands.append(Command_{ .replconf = .{ .capa_psync2 = {} } });
                                continue;
                            }
                        }
                    },
                    else => {
                        return error.CannotParseCommand;
                    },
                }
            }
        }
        return try self.commands.toOwnedSlice();
    }
};

test "parsing echo command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.echo, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "pineapple", cmd.echo);
    }
}

test "parsing config get command" {
    const bytes = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.config, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "dir", cmd.config.get);
    }
}

test "parsing psync command" {
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.psync, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "?", cmd.psync.replication_id);
        try std.testing.expectEqual(-1, cmd.psync.offset);
    }
}

test "parsing wait command" {
    const bytes = "*3\r\n$4\r\nWAIT\r\n$1\r\n5\r\n$3\r\n500\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.wait, std.meta.activeTag(cmd));
        try std.testing.expectEqual(5, cmd.wait.num_replicas_to_ack);
        try std.testing.expectEqual(500, cmd.wait.exp);
    }
}

test "parsing set command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.set, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "apple", cmd.set.key);
        try std.testing.expectEqualSlices(u8, "pear", cmd.set.val);
    }
}

test "parsing set command with expiry option" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.set, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "apple", cmd.set.key);
        try std.testing.expectEqualSlices(u8, "pear", cmd.set.val);
        try std.testing.expectEqual(100, cmd.set.px.?);
    }
}
test "parsing get command" {
    const bytes = "*2\r\n$3\r\nGET\r\n$5\r\napple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.get, std.meta.activeTag(cmd));
        try std.testing.expectEqualSlices(u8, "apple", cmd.get.key);
    }
}

test "parsing ping command" {
    const bytes = "*1\r\n$4\r\nPING\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.ping, std.meta.activeTag(cmd));
    }
}

test "parsing info command" {
    const bytes = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.info, std.meta.activeTag(cmd));
        try std.testing.expectEqual(Command_.Info.replication, cmd.info);
    }
}

test "parsing replconf listening-port command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
        try std.testing.expectEqual({}, cmd.replconf.listening_port);
    }
}

test "parsing replconf capa psync2 command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
        try std.testing.expectEqual({}, cmd.replconf.capa_psync2);
    }
}

test "parsing replconf ack command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmds = try parser.parse_();
    defer gpa.free(cmds);

    for (cmds) |cmd| {
        try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
        try std.testing.expectEqual(0, cmd.replconf.ack);
    }
}
