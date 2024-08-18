const std = @import("std");
const rand = std.crypto.random;
const net = std.net;
pub const Loc = struct { start: usize, end: usize };
pub const Tag = enum { echo, ping, set, get, info, replconf, psync };
pub const Command = struct { loc: Loc, tag: Tag, args: [3]Arg, opt: ?Arg = null };
pub const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };
pub const Role = enum { master, slave };

// TODO: flesh this out and replace current impl of Command
// const Command_ = union(enum) {
//     ping,
//     info,
//     set,
//     get,
//     psync,
//     replconf,
//
//     const Set = struct {};
// };
//
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
    replicas: [5]Replica, // should probably turn this into an array list
    role: Role = .master,
    replication_id: ?[]u8 = null,
    replica_count: u8 = 0,
    master_host: ?[]const u8 = null,
    master_port: ?u16 = null,
    port: u16 = 6379,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        return .{ .replicas = undefined, .allocator = allocator };
    }

    pub fn deinit(self: *ServerState) !void {
        if (self.replication_id != null) {
            self.allocator.free(self.replication_id.?);
        }
    }

    pub fn generate_master_replication_id(self: *ServerState) !void {
        var rep_id_slice = try self.allocator.alloc(u8, 40);
        var i: usize = 0;

        while (i < rep_id_slice.len) {
            const rand_int = rand.int(u8);

            if (std.ascii.isAlphanumeric(rand_int)) {
                rep_id_slice[i] = rand_int;
                i += 1;
            }
        }
        self.replication_id = rep_id_slice;
    }

    pub fn forward_cmd(self: *ServerState, cmd_buf: []const u8) !void {
        for (0..self.replica_count) |i| {
            try self.replicas[i].write(cmd_buf);
        }
    }

    pub fn add_replica(self: *ServerState, stream: net.Stream) void {
        self.replicas[self.replica_count] = Replica.init(stream);

        self.replica_count += 1;
    }
};
