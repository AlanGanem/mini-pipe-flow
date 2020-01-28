# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 18:57:23 2020

@author: User Ambev
"""
import pydot_ng as pydot
from .Nodes import Inputer, Node, Renamer
import pickle

def model_to_dot(graph,
                 show_layer_names=True,
                 rankdir='TB',
                 expand_nested=False,
                 dpi=96,
                 subgraph=False):

    def add_edge(dot, src, dst):
      if not dot.get_edge(src, dst):
        dot.add_edge(pydot.Edge(src, dst))

    #create dot object
    dot = pydot.Dot()
    dot.set('rankdir', rankdir)
    dot.set('concentrate', True)
    dot.set('dpi', dpi)
    dot.set_node_defaults(shape='record')

    for node in graph:
        label = '{}: {}'.format(node.__class__.__name__,node.name)
        mode = ''
        if node.fit_only:
            mode = 'fit_only'
        elif node.transform_only:
            mode = 'transform_only'
        else:
            mode = 'fit and transform'

        if not node.name in [None,'None','NoneType']:
            name = node.name
        else:
            name = 'Unnamed'

        type_ = node.estimator.__class__.__name__
        if type_ == 'NoneType':
            type_ = node.__class__.__name__

        #create node_labels

        if not node.is_callable:
            required_input_labels = node.required_inputs
            optional_input_labels = node.optional_inputs
            output_labels = node.allowed_outputs
        else:
            required_input_labels = node.required_inputs['fit']
            optional_input_labels = node.optional_inputs['fit']
            output_labels = node.allowed_outputs['fit']

        if isinstance(node, Inputer):
            label = "Inputer\n%s\n%s\n%s" % (
                name,
                mode,
                list(node.required_inputs['fit'])
            )
        elif isinstance(node, Renamer):
            label = "Renamer\n%s\n%s| %s" % (
                name,
                mode,
                node.mapper
            )
        else:
            label = "%s\n%s\n%s|{required_input:|optional_input:|output:}|{{%s}|{%s}|{%s}}" % (
                type_ ,
                name,
                mode,
                required_input_labels,
                optional_input_labels,
                output_labels)


        node = pydot.Node(node.name, label=label)
        dot.add_node(node)
    for edge in graph.edges:
        add_edge(dot, edge[0].name, edge[1].name)

    return dot

#recursively populate graph backwards
def populate_graph(node, graph):
    if not node in graph:
        graph.add_node(node)
    for in_node in node.input_nodes:
        if node.input_nodes:
            if not in_node in graph:
                graph.add_node(in_node)
            graph.add_edge(in_node,node)
            populate_graph(in_node,graph)
        else:
            if not in_node in graph:
                graph.add_node(in_node)
            graph.add_edge(in_node,node)


def run_pipeline(loading_path, inputs ,mode = 'transform',**runargs):

    with open(loading_path, 'rb') as file:
        pipe = pickle.load(file)

    result = getattr(pipe,mode)(
        inputs,
        **runargs
    )
    return result

