//! Consumer group coordination: shard assignment and leases.
//!
//! See also: [`crate::shard_router`], [`crate::group_subscribe`].

mod coordinator;
mod lease_store;
mod static_assignment;

pub use coordinator::{
    ConsumerGroupCoordinator, FleetGroupCoordinator, GroupMember, StaticGroupCoordinator,
};
pub use lease_store::{ConsumerLease, LeaseStore, MemoryLeaseStore};
pub use static_assignment::{
    instance_id_from_env, member_count_from_env, parse_shard_assignment,
    round_robin_shards_for_member, shard_count_from_env, static_assigned_shards,
    StaticAssignmentConfig,
};
