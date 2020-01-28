# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 14:12:06 2019

@author: User Ambev
"""


import pickle

import networkx as nx
from matplotlib import pyplot as plt
import datetime

from .Base import *
from .Nodes import *
from .Utils import populate_graph, model_to_dot


class Custom():
    ### IMPLEMENT __getitem__, __iteritem__, __repr__, __str__
    #### implement sequential mode

    @classmethod
    def load(cls, loading_path,**pickleargs):
        with open(loading_path, 'rb') as file:
            loaded_pipe = pickle.load(file, **pickleargs)
        return loaded_pipe
    
    def save(self, saving_path, **pickleargs):
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        with open(saving_path, 'wb') as file:
            pickle.dump(self, file, **pickleargs)

    def __init__(self,input_nodes, output_nodes):
        if not isinstance(input_nodes, list):
            raise TypeError('input_nodes must be list')

        if not isinstance(output_nodes, list):
            raise TypeError('output_nodes must be list')

        for node in input_nodes+output_nodes:
            if not isinstance(node, Capsula):
                raise TypeError('{} should be an instance of Capsula'.format(node.name))
        
        for node in output_nodes:
            if node.fit_only:
                raise ValueError('output nodes should be transformable. {} is a fit_only Node'.format(node.name))

        graph = self.build_graph(input_nodes,output_nodes)
        
        self.check_graph(graph,input_nodes, output_nodes)
        self.graph = graph        
        self.nodes = {node.name:node for node in self.graph}
        self.fit_graph = self.graph.subgraph([node for node in self.graph if not node.transform_only])
        self.transform_graph = self.graph.subgraph([node for node in self.graph if not node.fit_only])
        self.index_name_map = {node.name:index for index,node in enumerate(graph)}
        self.output_nodes = output_nodes
        self.creation_date = datetime.datetime.now()
        self.debugger = {}

        # def input_nodes dict
        self.input_nodes = {}
        self.input_nodes['fit'] = []
        self.input_nodes['transform'] = []
        for node in input_nodes:
            if not node.fit_only:
                self.input_nodes['transform'].append(node)
            if not node.transform_only:
                self.input_nodes['fit'].append(node)
        #####
        

        
    def get_nodes(self):
        nodes = list(self.graph.nodes)
        names = [node.name for node in nodes]
        return dict(zip(names, nodes))

    def __iter__(self):
        return iter(self.graph)

    def __getitem__(self, name):
        return (list(self.graph.nodes)[self.index_name_map[name]])


    def build_graph(self,input_nodes, output_nodes):
                    
        graph = nx.DiGraph()
        for output in output_nodes:
            populate_graph(output, graph)

        return graph


    def populate_input_nodes(self, inputs, pipe_call = None):
        if not pipe_call in ['fit','transform']:
            raise ValueError('pipe_call must be one of ["fit","transform"]')

        for node in self.input_nodes[pipe_call]:
            node.store(inputs[node.name])
            node.landing_zone = inputs[node.name]

    def fit(self, inputs = {}, debug_nodes = [],clear_nodes_memory = True):
        assert isinstance(inputs, dict)
        if debug_nodes:
            bad_names = [name for name in debug_nodes if name not in self.nodes.keys()]
            if bad_names:
                raise NameError('No node in self.graph is named {}'.format(bad_names))
        # check input nodes insatiated and values passed
        input_node_set = set([node.name for node in self.input_nodes['fit']])
        inputs_set = set(inputs.keys())
        if inputs_set != input_node_set:
            raise KeyError(r'fit inputs must contain keys {}. got {} instead'.format(input_node_set, inputs_set))

        # clear graph memory
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        #populate input_nodes
        if inputs:
            self.populate_input_nodes(inputs, pipe_call= 'fit')
            
        #fit nodes in topological sort
        self.debugger = {'metadata':{
            'pipe_call':'fit',
            'call_instant':datetime.datetime.now()
        }
        }
        for node in nx.algorithms.dag.topological_sort(self.fit_graph):
            for in_node in node.input_nodes:
                node.take('all', in_node)
            node.fit()
            node.transform()
            if node.name in debug_nodes:
                self.debugger[node.name] = {}
                self.debugger[node.name]['inputs'] = node.landing_zone
                self.debugger[node.name]['outputs'] = node.takeoff_zone

        if not clear_nodes_memory == False:
            self.clear_landing_zones()
            self.clear_takeoff_zones()
            
        self.is_fitted = True
        self.last_fit_date = datetime.datetime.now()
        return self     

    def transform(self, inputs = {} , debug_nodes = [],clear_nodes_memory = True):
        assert isinstance(inputs, dict)
        if debug_nodes:
            bad_names = [name for name in debug_nodes if name not in self.nodes.keys()]
            if bad_names:
                raise NameError('No node in self.graph is named {}'.format(bad_names))
        # check input nodes insatiated and values passed
        input_node_set = set([node.name for node in self.input_nodes['transform']])
        inputs_set = set(inputs.keys())
        if inputs_set != input_node_set:
            raise KeyError('transform inputs keys must be {}. got {} instead'.format(input_node_set,inputs_set))

        #clear graph memory
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        if inputs:
            self.populate_input_nodes(inputs, pipe_call= 'transform')

        # fit nodes in topological sort
        self.debugger = {'metadata': {
            'pipe_call': 'transform',
            'call_instant': datetime.datetime.now()
        }
        }
        for node in nx.algorithms.dag.topological_sort(self.transform_graph):
            for in_node in node.input_nodes:
                node.take('all', in_node)
            if not node.is_fitted:
                raise RuntimeError('{} have not been fitted yet'.format(node.name))
            node.transform()
            if node.name in debug_nodes:
                self.debugger[node.name] = {}
                self.debugger[node.name]['inputs'] = node.landing_zone
                self.debugger[node.name]['outputs'] = node.takeoff_zone
        
        outputs = {node.name:node.takeoff_zone for node in self.output_nodes}
        
        if not clear_nodes_memory == False:
            self.clear_landing_zones()
            self.clear_takeoff_zones()


        self.is_transformed = True
        self.last_transform_date = datetime.datetime.now()
        return outputs

    def debug(self,debug_nodes):
        for node_name in debug_nodes:
            if node_name == 'metadata':
                raise NameError('"metadata" is not a valid node name for debugging')
            self.debugger[node_name] = {}
            self.debugger[node_name]['inputs'] = self[node_name].landing_zone
            self.debugger[node_name]['outputs'] = self[node_name].takeoff_zone
        return
                            
    def check_graph(self,graph, input_nodes, output_nodes):
        # check if all n odes are instances of capsula
        non_capsula_nodes = [node for node in graph if not isinstance(node, Capsula)]
        if not all([isinstance(node,Capsula) for node in graph]):
            raise TypeError('all nodes should be and instance of capsula. {} are    not'.format(non_capsula_nodes))
        # check whether any Inputer is not passed in input_nodes
        inputers = [node for node in graph if isinstance(node,Inputer)]
        inputers_not_in_input_nodes = [i.name for i in inputers if i not in input_nodes]
        if inputers_not_in_input_nodes:
            raise TypeError('{} are Inputer nodes and should be passed as input_nodes in the pipeline constuctor'.format(inputers_not_in_input_nodes))
        #create fit and transform subgraph (only for checking)
        #fit_graph = graph.subgraph([node for node in graph.nodes if not node.transform_only])
        #transform_graph = graph.subgraph([node for node in graph.nodes if not node.fit_only])
        #check for connectedness and input nodes as endpoints
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(graph):
            self.plot_graph(graph = graph, with_labels = True)
            raise AssertionError('graph is disconnected')

        # if not nx.algorithms.components.weakly_connected.is_weakly_connected(fit_graph):
        #     self.plot_graph(graph = graph, with_labels = True)
        #     raise AssertionError('fit_graph is disconnected. {}'.format([node.name for node in fit_graph]))
        # if not nx.algorithms.components.weakly_connected.is_weakly_connected(transform_graph):
        #     self.plot_graph(graph = graph, with_labels = True)
        #     raise AssertionError('transform_graph is disconnected. {}'.format([node.name for node in transform_graph]))

        ###CHECK FOR DUPLICATE NAMES
        node_names = [node.name for node in graph]
        duplicates = [node for node in graph if node_names.count(node.name) >1]
        
        if duplicates:
            raise ValueError('node names should be unique. duplicated names in graph: {}'.format(
                list(zip(duplicates,[node.name for node in duplicates])))
                )

    def clear_landing_zones(self):
        for node in self.graph:
            node.clear_landing_zone()


    def clear_takeoff_zones(self):
        for node in self.graph:
            node.clear_takeoff_zone()

    
    def plot_graph(self, prog = 'dot', root = None ,graph = None,**drawargs):
        if graph == None:
            graph = self.graph
            
        for node in graph:
            node.color = 'b'
        for node in graph:
            if node.fit_only:
                node.color = 'g'
        for node in graph:
            if node.transform_only:    
                node.color = 'orange'
        
        color_map = [node.color for node in graph]
        nx.draw(graph, **drawargs, node_color = color_map, layout = nx.drawing.layout.kamada_kawai_layout(graph))
        plt.show()

    def plot_pipeline(self, file_path, **plotargs):
        # if not subgraph in ['all','fit','transform']:
        #     raise TypeError('subgraph must be one of {}'.format(['all','fit','transform']))
        
        # if subgraph == 'transform':
        #     graph = self.transform_graph
        # if subgraph == 'fit':
        #     graph = self.fit_graph
        # if subgraph == 'all':

        graph = self.graph

        dot = model_to_dot(graph, **plotargs)
        dot.write_png(file_path)


class Sequential(Custom):

    def __init__(self, estimators):
        input_nodes, output_nodes = self.encapsulate(estimators)
        super().__init__(
            input_nodes= input_nodes,
            output_nodes= output_nodes
        )

    def encapsulate(self, estimators):
        assert isinstance(estimators, list)
        i = 0
        estim_list = []
        for i, estim in enumerate(estimators):
            if i == 0:
                estim_list.append(Inputer())
            else:
                if isinstance(estim, Capsula):
                    setattr(estim, 'input_nodes', estim_list[i-1:i])
                    estim_list.append(estim)
                else:
                    estim_list.append(Capsula(estim, input_nodes = estim_list[i-1:i]))
            i+=1
        input_nodes = [estim_list[0]]
        output_nodes = [estim_list[-1]]
        return input_nodes, output_nodes