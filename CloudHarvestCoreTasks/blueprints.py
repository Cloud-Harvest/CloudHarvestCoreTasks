"""
This file provides the types of blueprints that can be registered by the CloudHarvestCorePluginManager:
    HarvestAgentBlueprint: a blueprint loaded for instances of CloudHarvestAgent.
    HarvestApiBlueprint: a blueprint loaded for instances of CloudHarvestApi

We make these distinctions because we don't want Agents to load API endpoints and vice versa.
"""

from flask import Blueprint
from CloudHarvestCorePluginManager.decorators import register_definition
from logging import getLogger

logger = getLogger('harvest')


@register_definition(name='agent', category='blueprint', register_instances=True)
class HarvestAgentBlueprint(Blueprint):
    def __init__(self, *args, **kwargs):
        logger.info(f'Initializing Blueprint: {args[0]}')

        super().__init__(*args, **kwargs)


@register_definition(name='api', category='blueprint', register_instances=True)
class HarvestApiBlueprint(Blueprint):
    def __init__(self, *args, **kwargs):
        logger.info(f'Initializing Blueprint: {args[0]}')

        super().__init__(*args, **kwargs)
