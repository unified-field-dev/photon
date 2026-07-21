//! Consumer group coordinator trait and implementations.

use async_trait::async_trait;

use crate::consumer_group::static_assignment;
use crate::error::Result;

/// Registered group member.
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Consumer group id.
    pub group_id: String,
    /// Unique member instance id within the group.
    pub instance_id: String,
    /// Topic this member consumes.
    pub topic_name: String,
    /// Virtual shard count for the topic.
    pub shard_count: u32,
}

/// Assigns virtual shards to group members.
#[async_trait]
pub trait ConsumerGroupCoordinator: Send + Sync {
    /// Register member and return assigned shard ids.
    async fn register(&self, member: GroupMember) -> Result<Vec<u32>>;

    /// Heartbeat to keep assignment alive (fleet lease store).
    async fn heartbeat(&self, group_id: &str, instance_id: &str) -> Result<()> {
        let _ = (group_id, instance_id);
        Ok(())
    }

    /// Current assignment for a member.
    async fn assigned_shards(&self, group_id: &str, instance_id: &str) -> Result<Vec<u32>>;
}

/// Env-based static assignment (`PHOTON_GROUP_SHARD_ASSIGNMENT`).
pub struct StaticGroupCoordinator;

#[async_trait]
impl ConsumerGroupCoordinator for StaticGroupCoordinator {
    async fn register(&self, member: GroupMember) -> Result<Vec<u32>> {
        Ok(static_assignment::static_assigned_shards(
            member.shard_count,
        ))
    }

    async fn assigned_shards(&self, group_id: &str, instance_id: &str) -> Result<Vec<u32>> {
        let _ = (group_id, instance_id);
        Ok(static_assignment::static_assigned_shards(
            static_assignment::shard_count_from_env().unwrap_or(32),
        ))
    }
}

/// Fleet coordinator backed by a [`super::lease_store::LeaseStore`].
pub struct FleetGroupCoordinator<L: super::lease_store::LeaseStore> {
    store: L,
    lease_ttl_secs: u64,
}

impl<L: super::lease_store::LeaseStore> FleetGroupCoordinator<L> {
    /// Create a coordinator with the given lease store and lease TTL.
    pub const fn new(store: L, lease_ttl_secs: u64) -> Self {
        Self {
            store,
            lease_ttl_secs,
        }
    }

    fn range_assign(member_index: u32, member_count: u32, shard_count: u32) -> Vec<u32> {
        if member_count == 0 {
            return Vec::new();
        }
        let per = shard_count.div_ceil(member_count);
        let start = member_index * per;
        let end = (start + per).min(shard_count);
        (start..end).collect()
    }
}

#[async_trait]
impl<L: super::lease_store::LeaseStore> ConsumerGroupCoordinator for FleetGroupCoordinator<L> {
    async fn register(&self, member: GroupMember) -> Result<Vec<u32>> {
        let member_index = member.instance_id.parse::<u32>().unwrap_or_else(|_| {
            u32::try_from(member.instance_id.len()).unwrap_or(0) % member.shard_count.max(1)
        });
        let member_count = static_assignment::member_count_from_env().unwrap_or(2);
        let shards = Self::range_assign(member_index, member_count, member.shard_count);
        for shard_id in &shards {
            self.store
                .claim(super::lease_store::ConsumerLease {
                    group_id: member.group_id.clone(),
                    shard_id: *shard_id,
                    instance_id: member.instance_id.clone(),
                    ttl_secs: self.lease_ttl_secs,
                })
                .await?;
        }
        Ok(shards)
    }

    async fn heartbeat(&self, group_id: &str, instance_id: &str) -> Result<()> {
        self.store
            .renew(group_id, instance_id, self.lease_ttl_secs)
            .await
    }

    async fn assigned_shards(&self, group_id: &str, instance_id: &str) -> Result<Vec<u32>> {
        self.store.list_for_instance(group_id, instance_id).await
    }
}
