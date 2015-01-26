/*
 * Zoned block device handling
 *
 * Copyright (c) 2015, Hannes Reinecke
 * Copyright (c) 2015, SUSE Linux GmbH
 */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/blkdev.h>
#include <linux/rbtree.h>

struct blk_zone *blk_lookup_zone(struct request_queue *q, sector_t lba)
{
	struct rb_root *root = &q->zones;
	struct rb_node *node = root->rb_node;

	while (node) {
		struct blk_zone *zone = container_of(node, struct blk_zone,
						     node);

		if (lba < zone->start)
			node = node->rb_left;
		else if (lba >= zone->start + zone->len)
			node = node->rb_right;
		else
			return zone;
	}
	return NULL;
}
EXPORT_SYMBOL_GPL(blk_lookup_zone);

struct blk_zone *blk_insert_zone(struct request_queue *q, struct blk_zone *data)
{
	struct rb_root *root = &q->zones;
	struct rb_node **new = &(root->rb_node), *parent = NULL;

	/* Figure out where to put new node */
	while (*new) {
		struct blk_zone *this = container_of(*new, struct blk_zone,
						     node);
		parent = *new;
		if (data->start + data->len <= this->start)
			new = &((*new)->rb_left);
		else if (data->start >= this->start + this->len)
			new = &((*new)->rb_right);
		else {
			/* Return existing zone */
			return this;
		}
	}
	/* Add new node and rebalance tree. */
	rb_link_node(&data->node, parent, new);
	rb_insert_color(&data->node, root);

	return NULL;
}
EXPORT_SYMBOL_GPL(blk_insert_zone);

void blk_drop_zones(struct request_queue *q)
{
	struct rb_root *root = &q->zones;
	struct blk_zone *zone, *next;

	rbtree_postorder_for_each_entry_safe(zone, next, root, node) {
		kfree(zone);
	}
	q->zones = RB_ROOT;
}
EXPORT_SYMBOL_GPL(blk_drop_zones);
