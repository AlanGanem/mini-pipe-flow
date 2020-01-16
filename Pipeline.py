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

    def __init__(self,input_nodes, output_nodes , clear_zones = None):
        if not isinstance(input_nodes, list):
            raise TypeError('input_nodes must be list')

        if not isinstance(output_nodes, list):
            raise TypeError('output_nodes must be list')

        for node in input_nodes+output_nodes:
            if not isinstance(node, Capsula):
                raise TypeError('{} should be an instance of Capsula'.format(node.name))
        
        
        self.clear_zones = clear_zones
        graph = self.build_graph(input_nodes,output_nodes)
        
        self.check_graph(graph,input_nodes, output_nodes)
        self.graph = graph        
        self.nodes = [node for node in self.graph]
        self.fit_graph = self.graph.subgraph([node for node in self.nodes if not node.transform_only])
        self.transform_graph = self.graph.subgraph([node for node in self.nodes if not node.fit_only])
        
        self.index_name_map = {node.name:index for index,node in enumerate(graph)}
        self.input_nodes = input_nodes
        self.output_nodes = output_nodes
        self.creation_date = datetime.datetime.now()
        

        
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


    def populate_input_nodes(self, inputs):
        for node in self.input_nodes:
            node.store(inputs[node.name])

    def fit(self, inputs = {}, clear_zones = True):
        assert isinstance(inputs, dict)
        # check input nodes insatiated and values passed
        input_node_set = set([node.name for node in self.input_nodes if not node.transform_only])
        inputs_set = set(inputs.keys())
        if inputs_set != input_node_set:
            raise KeyError(r'inputs must contain keys {}. got {} instead'.format(input_node_set, inputs_set))

        # clear graph memory
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        #populate input_nodes
        if inputs:
            self.populate_input_nodes(inputs)
            
        #recursively fit each output_node
        for node in self.output_nodes:
            self.recursive_fit([node], pipe_call = 'fit')

        if clear_zones == True:
            self.clear_landing_zones()
            self.clear_takeoff_zones()
            
        self.is_fitted = True
        self.last_fit_date = datetime.datetime.now()
        return self     

    def transform(self, inputs = {}, clear_zones = True):
        assert isinstance(inputs, dict)
        # check input nodes insatiated and values passed
        input_node_set = set([node.name for node in self.input_nodes if not node.fit_only])
        inputs_set = set(inputs.keys())
        if inputs_set != input_node_set:
            raise KeyError('inputs keys must be {}. got {} instead'.format(input_node_set,inputs_set))

        #clear graph memory
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        if inputs:
            self.populate_input_nodes(inputs)

        # recursively transform each output_node
        outputs = []
        for node in self.output_nodes:
            self.recursive_transform([node], pipe_call = 'transform')
            outputs.append(node.takeoff_zone)
            
        if clear_zones == True:
            self.clear_landing_zones()
            self.clear_takeoff_zones()

        self.is_transformed = True
        self.last_transform_date = datetime.datetime.now()
        return outputs
                            
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
        fit_graph = graph.subgraph([node for node in graph.nodes if not node.transform_only])
        transform_graph = graph.subgraph([node for node in graph.nodes if not node.fit_only])
        #check for connectedness and input nodes as endpoints
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(graph):
            self.plot_graph(graph = graph, with_labels = True)
            raise AssertionError('graph is disconnected')
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(fit_graph):
            self.plot_graph(graph = graph, with_labels = True)
            raise AssertionError('fit_graph is disconnected. {}'.format([node.name for node in fit_graph]))
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(transform_graph):
            self.plot_graph(graph = graph, with_labels = True)
            raise AssertionError('transform_graph is disconnected. {}'.format([node.name for node in transform_graph]))

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

    def plot_pipeline(self, file_path, subgraph = 'all' ,**plotargs):
        if not subgraph in ['all','fit','transform']:
            raise TypeError('subgraph must be one of {}'.format(['all','fit','transform']))
        
        if subgraph == 'transform':
            graph = self.transform_graph
        if subgraph == 'fit':
            graph = self.fit_graph
        if subgraph == 'all':
            graph = self.graph
        
        
        dot = model_to_dot(graph, **plotargs)
        dot.write_png(file_path)

    def recursive_fit(self, nodes, pipe_call = None):
        if pipe_call == 'transform':
            nodes = [node for node in nodes if not node.fit_only]
        elif pipe_call == 'fit':
            nodes = [node for node in nodes if not node.transform_only]
        else:
            raise ValueError('pipe_call must be one of ["transform","fit"]')
        
        for node in nodes:
            required_inputs = node.required_inputs['fit']
            node_predecessors = [node for node in self.graph.predecessors(node) if not node.transform_only]
            node_successors = [node for node in self.graph.successors(node) if not node.transform_only]

            if not node.is_fitted:
                if node.landingzone_ready(required_inputs):
                    node.fit()
                    self.recursive_fit(node_successors, pipe_call = pipe_call)
                if not node.landingzone_ready(required_inputs):
                    for pre_node in node_predecessors:
                        if pre_node.is_transformed:
                            node.take(variables = 'all', sender = pre_node)
                        if not pre_node.is_transformed:
                            if pre_node.is_fitted:
                                self.recursive_transform([pre_node], pipe_call = pipe_call)
                                node.take(variables = 'all', sender = pre_node)
                            if not pre_node.is_fitted:
                                self.recursive_fit([pre_node], pipe_call = pipe_call)
                                self.recursive_transform([pre_node], pipe_call = pipe_call)
                                node.take(variables = 'all', sender = pre_node)
                    if node.landingzone_ready(required_inputs):
                        print('inputs, apply recursive fit in {}'.format(node.name))
                        self.recursive_fit([node], pipe_call = pipe_call)
                    else:
                        inputs_in_lz = set(node.landing_zone)
                        missing_inputs = set(node.required_inputs['fit']) - inputs_in_lz
                        raise AssertionError('{} missing required_inputs {}'.format(node.name,missing_inputs))
        return

    def recursive_transform(self, nodes, pipe_call = None):
        if pipe_call == 'transform':
            nodes = [node for node in nodes if not node.fit_only]
        elif pipe_call == 'fit':
            nodes = [node for node in nodes if not node.transform_only]
        else:
            raise ValueError('pipe_call must be one of ["transform","fit"]')
        
        for node in nodes:
            required_inputs = node.required_inputs['transform']
            node_predecessors = [node for node in self.graph.predecessors(node) if not node.fit_only]
            node_successors = [node for node in self.graph.successors(node) if not node.fit_only]
            print('recursive_transformed called by {}'.format(node.name))
            if node.is_fitted:
                if not node.is_transformed:
                    if node.landingzone_ready(required_inputs):
                        if not node.is_transformed:
                            node.transform()
                            self.recursive_transform(node_successors, pipe_call = pipe_call)
                    else:
                        for pre_node in node_predecessors:
                            node.take('all', pre_node)
                        if node.landingzone_ready(required_inputs):
                            node.transform()
                            self.recursive_transform(node_successors, pipe_call = pipe_call)
                        else:
                            self.recursive_transform(node_predecessors, pipe_call = pipe_call)
                            for pre_node in node_predecessors:
                                node.take('all', pre_node)
                            node.transform()
                            self.recursive_transform(node_successors, pipe_call = pipe_call)
                else:
                    return

            else:
                return
                #raise AssertionError('{} is not fitted'.format(node.name))



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