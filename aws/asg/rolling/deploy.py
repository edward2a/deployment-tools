#!/usr/bin/env python3
"""
AWS AutoScaling Rolling Deployment.

This program will list all ec2 instances within an AutoScaling group and
execute a predefined task.

The task can range from executing something inside the instances (provided
access is available) or rotate the instances.
"""

import argparse
import boto3
import logging
import shelve
import os

from time import sleep


def parse_args():
    """CLI argument parser."""
    p = argparse.ArgumentParser()

    p.add_argument('-b', '--batch', required=False, default=1, type=int,
        help='Batch size for update')
    p.add_argument('-g', '--group', required=True,
        help='AutoScaling group name')
    p.add_argument('-r', '--region', required=True,
        help='Target AWS region')
    p.add_argument('-s', '--state', required=False, default='dynamic',
        help='The deployment state file')
    # FUTURE USE
    #p.add_argument('--resume', required=False, default=False, action='store_true',
    #    help='Enable to resume from a previously saved state file')

    args = p.parse_args()
    if args.state == 'dynamic':
        args.state = args.group + '.state'

    return args


class Inventory(object):
    """ASG/EC2 Instance Inventory Manager."""

    def __init__(self, asg):
        """Init."""
        self.asg = asg

    def new_instance_state(self, asg_name, store_file):
        """Generate a new instace list from the data obtained from AWS."""
        logger.info('Storing instance state for autoscaling group {}...'.format(asg_name))
        self.state = shelve.open(store_file)
        for i in self.query_asg(asg_name)['Instances']:
            logger.info('Adding instance {}...'.format(i['InstanceId']))
            self.state[i['InstanceId']] = 'pending_replacement'

    def update_state(self, instance, state):
        """Update the instance state in the persistent store."""
        self.state[instance] = state
        self.state.sync()

    def query_asg(self, asg_name):
        """Return a dict containing ASG data."""
        logger.info('Feching autoscaling group data for {}...'.format(asg_name))
        groups, resp = [item for item in self.asg.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]).values()]
        if len(groups) == 1:
            logger.info('Found!')
            return groups[0]
        elif len(groups) == 0:
            logger.error('AutoScaling group {} not found!'.format(asg_name))
            exit(1)

    def get_inventory(self):
        """Return a dict of the state shelve."""
        return dict(self.state)


class Deploy(object):
    """Main Deployer Class."""

    def __init__(self, inventory, ec2, asg, target_asg):
        """Init."""
        self.inventory = inventory
        self.ec2 = ec2
        self.asg = asg
        self.target_asg = target_asg

    def trigger_instance_removal(self, instance_id):
        """Set instance to unhealthy to trigger removal."""
        logger.info('Triggering replacement of instance {}...'.format(instance_id))
        self.asg.set_instance_health(InstanceId=instance_id, HealthStatus='Unhealthy')

    def monitor_instance_termination(self, instance_ids):
        """Poll instances until termination."""
        state = lambda i: self.ec2.Instance(id=i)
        monitored = {}

        for instance in instance_ids:
            monitored[instance] = state(instance)

        logger.info('Waiting for termination of instances: {}...'.format(' '.join(monitored.keys())))
        while len(monitored) > 0:
            sleep(15)
            logger.info('...')
            for i in list(monitored.keys()):
                monitored[i].reload()
                if monitored[i].state['Name'] == 'terminated':
                    self.inventory.state[i] = 'terminated'
                    del monitored[i]
        logger.info('Done!')

    def execute(self, batch, store_file):
        logger.info('Processing batch size of {}'.format(batch))
        self.inventory.new_instance_state(self.target_asg, store_file)
        total_instances = len(self.inventory.state)
        logger.info('Total instances to replace: {}'.format(total_instances))
        instance_index = 0
        while instance_index < total_instances:
            logging.info('Pending instances to replace: {}'.format(total_instances - instance_index))
            remove = list(self.inventory.state.keys())[instance_index:instance_index + batch]
            logger.debug('removal list: ' + str(remove))
            instance_index += batch
            for i in remove:
                self.trigger_instance_removal(i)
            self.monitor_instance_termination(remove)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    args = parse_args()
    logger.debug(args)
    ec2 = boto3.resource('ec2', region_name=args.region)
    asg = boto3.client('autoscaling', region_name=args.region)
    d = Deploy(Inventory(asg), ec2, asg, args.group)
    d.execute(args.batch, args.state)
    os.unlink(args.state)
