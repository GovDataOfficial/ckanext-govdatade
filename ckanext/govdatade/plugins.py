""" register plugin things here """
from ckan import plugins as p
import ckan.plugins.toolkit as tk
import ckanext.govdatade.commands.cli as cli


class GovDataDePlugin(p.SingletonPlugin):
    """ Init Plugin """

    p.implements(p.IClick)
    # IClick
    def get_commands(self):
        """ Get click commands """
        return cli.get_commands()
