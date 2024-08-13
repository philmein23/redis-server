const std = @import("std");
const net = std.net;
pub const Loc = struct { start: usize, end: usize };
pub const Tag = enum { echo, ping, set, get, info, replconf, psync };
pub const Command = struct { loc: Loc, tag: Tag, args: [2]Arg, opt: ?Arg = null };
pub const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };
pub const Role = enum { master, slave };

pub const Replica = struct {
    stream: net.Stream,

    pub fn init(stream: net.Stream) Replica {
        return .{ .stream = stream };
    }

    pub fn write(self: *Replica, cmd_buf: []const u8) !void {
        _ = try self.stream.write(cmd_buf);
    }
};

pub const ServerState = struct {
    replicas: [5]Replica,
    role: Role = .master,
    replication_id: ?[]u8 = null,
    replica_count: u8 = 0,
    master_host: ?[]const u8 = null,
    master_port: ?u16 = null,

    pub fn init() ServerState {
        return .{ .replicas = undefined };
    }

    pub fn forward_cmd(self: *ServerState, cmd_buf: []const u8) !void {
        for (0..self.replica_count) |i| {
            std.debug.print("FORWARDING CMD: {s}", .{cmd_buf});
            try self.replicas[i].write(cmd_buf);
        }
    }

    pub fn add_replica(self: *ServerState, stream: net.Stream) void {
        self.replicas[self.replica_count] = Replica.init(stream);

        self.replica_count += 1;
    }
};
