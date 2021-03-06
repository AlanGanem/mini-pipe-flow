import pickle
from abc import ABC, abstractmethod
import warnings
import inspect
import re

### Capsula not checking properly optional and required arguments of methods and functions

class Capsula():
    
    def __init__(
        self,
        estimator,
        name,
        fit_method = 'fit',
        fit_only = False,
        transform_method = 'transform',
        transform_only = False,
        required_inputs = [],
        optional_inputs = [],
        allowed_outputs = [],
        filter_inputs = True,
        none_estim_outputs_fit = None,
        none_estim_outputs_transform = None
        ):

        if isinstance(estimator, self.__class__):
            raise TypeError('estimator should not be an instance of {}'.format(self.__class__))

        if estimator.__class__ in [type,str,dict,list,set,tuple]:
            raise TypeError('{} estimator should be an instantiated object or callable, not {}'.format(estimator, estimator.__class__))
        
        if fit_only and transform_only:
            raise AssertionError('fit_only and transform_only cannot be assigned simutaneously')
        
        is_callable = False
        if callable(estimator):
            is_callable = True

        if is_callable:
            transform_method = '__call__'
            fit_method = '__call__'

        if not isinstance(name,str):
            try:
                name = estimator.__name__
            except:
                name = estimator.__class__.__name__
        if name == 'NoneType':
            name = 'Unnamed'


        if estimator != None:
            if not required_inputs:
                inputs = get_inputs(
                    is_callable = is_callable,
                    estimator = estimator,
                    fit_method = fit_method,
                    transform_method = transform_method
                )

                required_inputs = inputs['required_inputs']
                optional_inputs = inputs['optional_inputs']
        
        
        if not allowed_outputs:
            allowed_outputs = get_output_json_keys(
                estimator = estimator,
                transform_method = transform_method,
                fit_method = fit_method,
                none_estim_outputs_fit = none_estim_outputs_fit,
                none_estim_outputs_transform = none_estim_outputs_fit

            )

        output_name = {}
        output_name['fit'] = allowed_outputs['fit']
        output_name['transform'] = allowed_outputs['transform']

        ##### define everythong before this line
        self.estimator = estimator
        self.name = name
        self.fit_method = fit_method
        self.transform_method = transform_method
        self.fit_only = fit_only
        self.transform_only = transform_only
        self.required_inputs = required_inputs
        self.optional_inputs = optional_inputs
        self.allowed_outputs = allowed_outputs
        self.filter_inputs = filter_inputs
        self.output_name = output_name

        # set states unsetable by __init__ args
        self.input_nodes = set()
        self.output_nodes = set()
        if not is_callable:
            self.is_fitted = False
            self.is_transformed = False
        else:
            self.is_fitted = True
            self.is_transformed = False            
        self.is_callable = is_callable
        self.required_inputs_landed = False
        self.estimator_fitargs = {}
        self.estimator_transformargs = {}
        self.departures = []
        self.landing_zone = {}
        self.takeoff_zone = {}
        self.none_estim_outputs_fit = none_estim_outputs_fit
        self.none_estim_outputs_transform = none_estim_outputs_transform
        

        return

    def __str__(self):
        return self.name
    
    def set_fitargs(self, **kwargs):
        self.estimator_fitargs = {**self.estimator_fitargs, **kwargs}

    def reset_fitargs(self):
        self.estimator_fitargs = {}

    def set_transformargs(self, **kwargs):
        self.estimator_transformargs = {**self.estimator_transformargs, **kwargs}

    def reset_transformargs(self, **kwargs):
        self.estimator_transformargs = {}

    
    def __call__(self, input_nodes):
        self.add_input_nodes(input_nodes)
        return self

    def add_input_nodes(self, input_nodes):
        if not any(isinstance(input_nodes,check_class) for check_class in [list,set,tuple]):
            input_nodes = [input_nodes]
        for node in input_nodes:
            if isinstance(node, Capsula):
                self.input_nodes.add(node)
            else:
                raise TypeError('all input nodes must be and instance of {}. {} is {}'.format(Capsula, node, node.__class__,__name__))

    def remove_input_nodes(self, node_names):
        if not any(isinstance(node_names,check_class) for check_class in [list,set,tuple]):
            node_names = [node_names]
        names_dict = {node.name:node for node in self.input_nodes}
        for name in node_names:
            if isinstance(name, str):
                try:
                    self.input_nodes.remove(names_dict[name])
                except KeyError:
                    raise KeyError('{} is not an input node to {}'.format(name,self.name))
            else:
                raise TypeError('node_names must be a string or a container of strings, not {}'.format(name))

    def hatch(self):
        return self.estimator

    def send(self,variables, to_node):

        if variables == 'all':
            out = self.takeoff_zone
            self.departures.append(to_node)
            self.output_nodes.add(to_node)
            return out
        else:
            assert isinstance(variables,list)
            out = {out:self.takeoff_zone[out] for out in self.takeoff_zone if out in variables}
            self.departures.append(to_node)
            self.output_nodes.add(to_node)
            return out


    def store(self, values):

        storing_colisions =  set(self.takeoff_zone).intersection(set(values))
        if storing_colisions:
            warnings.warn('an output colision occured in {} takeoff_zone with the following variables: {}. old values will be overwritten.'.format(self.name,storing_colisions))
        self.takeoff_zone = {**self.takeoff_zone,**values}
        #print('{} stored in {} takeoffzone'.format(set(self.takeoff_zone),self.name))

    def take(self, variables, sender, colision_mode = 'warn'):
        
        inputs = sender.send(
            variables = variables,
            to_node = self.name,
        )
        landing_intersection = set(self.landing_zone).intersection(inputs)
        if landing_intersection:
            if colision_mode == 'warn':
                warnings.warn('an input colision occured in the landing zone with the following variables: {}. old values will be overwritten.\nfrom {} to {}'.format(landing_intersection,sender.name,self.name))
            elif colision_mode == 'raise':
                raise ValueError('An colision occured in landing zone with variable {}.\nfrom {} to {}'.format(landing_intersection,sender.name,self.name))
        
        self.landing_zone = {**self.landing_zone,**inputs}
    
    def clear_landing_zone(self):
        self.landing_zone = {}

    def clear_takeoff_zone(self):
        self.takeoff_zone = {}
        self.departures = []
        self.is_transformed = False
        #self.output_nodes = set({})


    def landingzone_ready(self, params, type_error_return = True):
        try:
            if set(params).issubset(set(self.landing_zone)):
                return True
            else:
                #print('missing params in {} : '.format(self.name) + str(set(params) - set(self.landing_zone)))
                return False
        except TypeError:
            print('{} is not a valid input type for {}. landingzone_ready will return {}'.format(params,self.name, type_error_return))
            return 

    def fit(self):

        inputs = self.landing_zone
        #filter inputs
        if self.filter_inputs:
            inputs = {key:value for key,value in inputs.items()
                      if key in self.required_inputs['fit']+
                      self.optional_inputs['fit']
                      }

        print('fitting {}'.format(self.name))
        #if theres a colision , fitargs will be used instead of inputs
        getattr(
            self.estimator,
            self.fit_method
            )(
                **inputs,
                **self.estimator_fitargs
            )

        self.is_fitted = True
        return self.hatch()
        

    # def __setitem__(self,key,value):
    #     setattr(self,key,value)
    #     return

    def transform(self):

        inputs = self.landing_zone
        #filter inputs
        if self.filter_inputs:
            inputs = {key:value for key,value in inputs.items()
                      if key in self.required_inputs['transform']+
                      self.optional_inputs['transform']}

        # if theres a colision , transformargs will be used instead of inputs
        output = getattr(
            self.estimator,
            self.transform_method
            )(
                **inputs,
                **self.estimator_transformargs
            )

        if not isinstance(output, dict):
            var_name = inspect_output(
                estimator = self.estimator,
                fit_method = self.fit_method,
                transform_method = self.transform_method)['transform']
            output = self.output_wrapper(output, var_name = var_name)
            #warnings.warn(('{} output type is {} instead of {}.\noutput have been wrapped in dict with key {}'.format(self.name, type(output), 'dict', str(self.output_name['transform']))))

        self.store(output)
        self.is_transformed = True
        return output

    def output_wrapper(self, item ,var_name):
        self.wrapped_output = True
        return {str(var_name):item}

    def bypass(self):

        inputs = self.landing_zone
        self.clear_takeoff_zone()
        self.store(inputs)

        return inputs


def get_inputs(
        is_callable,
        estimator,
        fit_method,
        transform_method
):

    required_inputs = {}
    optional_inputs = {}
    inspectobjs = {}

    if not is_callable:
        inspectobjs['fit'] = inspect.getfullargspec(getattr(estimator, fit_method))
        inspectobjs['transform'] = inspect.getfullargspec(getattr(estimator, transform_method))
        try:
            required_inputs['fit'] = inspectobjs['fit'].args[1:][:-len(inspectobjs['fit'].defaults)]
        except:
            required_inputs['fit'] = inspectobjs['fit'].args[1:]
        try:
            required_inputs['transform'] = inspectobjs['transform'].args[1:][
                                           :-len(inspectobjs['transform'].defaults)]
        except:
            required_inputs['transform'] = inspectobjs['transform'].args[1:]

        try:
            optional_inputs['fit'] = inspectobjs['fit'].args[1:][-len(inspectobjs['fit'].defaults):]
        except:
            optional_inputs['fit'] = ['']
        try:
            optional_inputs['transform'] = inspectobjs['transform'].args[1:][-len(inspectobjs['transform'].defaults):]
        except:
            optional_inputs['transform'] = ['']
    else:
        inspectobjs['fit'] = inspect.getfullargspec(estimator)
        inspectobjs['transform'] = inspectobjs['fit']
        if not inspect.ismethod(estimator):
            try:
                required_inputs['fit'] = inspectobjs['fit'].args[:-len(inspectobjs['fit'].defaults)]
            except:
                required_inputs['fit'] = inspectobjs['fit'].args
            try:
                required_inputs['transform'] = inspectobjs['transform'].args[
                                               :-len(inspectobjs['transform'].defaults)]
            except:
                required_inputs['transform'] = inspectobjs['transform'].args

            try:
                optional_inputs['fit'] = inspectobjs['fit'].args[-len(inspectobjs['fit'].defaults):]
            except:
                optional_inputs['fit'] = ['']
            try:
                optional_inputs['transform'] = inspectobjs['transform'].args[
                                               -len(inspectobjs['transform'].defaults):]
            except:
                optional_inputs['transform'] = ['']
        else:
            try:
                required_inputs['fit'] = inspectobjs['fit'].args[1:][:-len(inspectobjs['fit'].defaults)]
            except:
                required_inputs['fit'] = inspectobjs['fit'].args[1:]
            try:
                required_inputs['transform'] = inspectobjs['transform'].args[1:][
                                               :-len(inspectobjs['transform'].defaults)]
            except:
                required_inputs['transform'] = inspectobjs['transform'].args[1:]

            try:
                optional_inputs['fit'] = inspectobjs['fit'].args[1:][-len(inspectobjs['fit'].defaults):]
            except:
                optional_inputs['fit'] = ['']
            try:
                optional_inputs['transform'] = inspectobjs['transform'].args[1:][-len(inspectobjs['transform'].defaults):]
            except:
                optional_inputs['transform'] = ['']

    return {'required_inputs':required_inputs, 'optional_inputs':optional_inputs}


def get_output_json_keys(
        estimator,
        fit_method,
        transform_method,
        none_estim_outputs_fit,
        none_estim_outputs_transform
):
    
    if estimator != None:
        json_str = inspect_output(estimator = estimator, fit_method = fit_method, transform_method = transform_method)
        allowed_outputs = {}
        
        if ('{' in json_str['fit']) and (':' in json_str['fit']):
            values = re.findall('"([^"]+?)"\s*:', json_str['fit'])
            allowed_outputs['fit'] = values
        else:
            allowed_outputs['fit'] = [json_str['fit']]
        
        if ('{' in json_str['transform']) and (':' in json_str['transform']):
            values = re.findall('"([^"]+?)"\s*:', json_str['transform'])
            allowed_outputs['transform'] = values
        else:
            allowed_outputs['transform'] = [json_str['transform']]
    else:
        allowed_outputs = {}
        allowed_outputs['fit'] = none_estim_outputs_fit
        allowed_outputs['transform'] = none_estim_outputs_transform

    return allowed_outputs

def multiple_split(string, substring):
    for sub in substring:
        if isinstance(string, str):
            string = string.split(sub)
        elif isinstance(string, list):
            string = sum([st.split(sub) for st in string], [])
    string = [i for i in string if i != '']
    return string

def inspect_output(estimator, fit_method, transform_method):
    source_code = {}
    json_str = {}
    if transform_method != '__call__':
        source_code['fit'] = inspect.getsource(getattr(estimator, fit_method))
        source_code['transform'] = inspect.getsource(getattr(estimator, transform_method))
    else:
        source_code['fit'] = inspect.getsource(estimator)
        source_code['transform'] = inspect.getsource(estimator)
    
    json_str['fit'] = source_code['fit'].split('return')[-1]
    json_str['fit'] = json_str['fit'].replace(' ', '')
    json_str['fit'] = json_str['fit'].replace("'", '"')
    json_str['fit'] = json_str['fit'].replace("\n", '')

    json_str['transform'] = source_code['transform'].split('return')[-1]
    json_str['transform'] = json_str['transform'].replace(' ', '')
    json_str['transform'] = json_str['transform'].replace("'", '"')
    json_str['transform'] = json_str['transform'].replace("\n", '')
    return json_str