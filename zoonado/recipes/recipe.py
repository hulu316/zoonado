from __future__ import unicode_literals

from tornado import gen

from zoonado import exc


class Recipe(object):

    sub_recipes = {}

    def __init__(self, base_path="/"):
        self.client = None
        self.base_path = base_path

        for attribute_name, recipe_class in self.sub_recipes.items():
            recipe_args = ["base_path"]
            if isinstance(recipe_class, tuple):
                recipe_class, recipe_args = recipe_class

            recipe_args = [getattr(self, arg) for arg in recipe_args]

            recipe = recipe_class(*recipe_args)

            setattr(self, attribute_name, recipe)

    def set_client(self, client):
        self.client = client
        for sub_recipe in self.sub_recipes:
            getattr(self, sub_recipe).client = client

    @classmethod
    def validate_dependencies(cls):
        return True

    @gen.coroutine
    def ensure_path(self):
        yield self.client.ensure_path(self.base_path)

    @gen.coroutine
    def create_znode(self, path):
        try:
            yield self.client.create(path)
        except exc.NodeExists:
            pass
        except exc.NoNode:
            try:
                yield self.ensure_path()
            except exc.NodeExists:
                pass
