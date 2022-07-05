""" register plugin things here """
from ckan import plugins as p
import ckan.plugins.toolkit as tk

class GovDataDePlugin(p.SingletonPlugin):
    """ Init Plugin """

    if tk.check_ckan_version('2.9'):
        p.implements(p.IClick)
        # IClick
        def get_commands(self):
            """ Get click commands """
            from ckanext.govdatade.commands.cli import get_commands
            return get_commands()
