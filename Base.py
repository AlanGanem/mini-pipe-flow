import pickle
from abc import ABC, abstractmethod
import warnings
import inspect
import re

### Capsula not checking properly optional and required arguments of methods and functions

class BaseEstimator(ABC):

    @classmethod
    def load(cls, loading_path, **pickleargs):
        with open(loading_path, 'rb') as file:
            loaded_pipe = pickle.load(file, **pickleargs)
        return loaded_pipe

    def save(self, saving_path, **pickleargs):
        with open(saving_path, 'wb') as file:
            pickle.dump(self, file, **pickleargs)
            
                
    def __init__(
        self,
        inputs = None,
        outputs = None,
        name = None,
        input_check_mode = 'filter',
        output_check_mode = 'filter',
        fitargs = {},
        transformargs = {},
        **estimator_args
        ):
        
        if inputs:
            assert isinstance(inputs, list)
        if outputs:
            assert isinstance(outputs, list)
        
        
        if name:
            self.__name__ = name
        
        self.inputs = inputs
        self.outputs = outputs
        self.input_check_mode = input_check_mode
        self.output_check_mode = output_check_mode
        self.estimator_args = estimator_args

        return

    def check_input(self, inputs):
        
        assert isinstance(inputs, dict)        
        
        if not isinstance(self.inputs, list):
            print('must assign the allowed inputs in constructor (list). not checking performed.')
            return        

        if self.input_check_mode == 'filter':
            inputs = {input_name:value in input_name in self.inputs for input_name,value in inputs.items()}
            return inputs
        
        elif self.input_check_mode == 'raise':
            if not set(self.inputs) == set(inputs):
                print('input json must contain exactly {} keys'.format(self.inputs))
                raise AssertionError
        
        elif self.input_check_mode == 'ignore':
            return

        else:
            print('input_check_mode should be one of ["filter","raise","ignore"]')
            raise ValueError

    def check_output(self, outputs):
        
        assert isinstance(outputs, dict)        
        
        if not isinstance(self.outputs, list):
            print('must assign the allowed outputs in constructor (list). not checking performed.')
            return

        if self.output_check_mode == 'filter':
            outputs = {output_name:value in output_name in self.outputs for output_name,value in outputs.items()}
            return outputs        
        
        elif self.output_check_mode == 'raise':
            if not set(self.outputs) == set(outputs):
                print('input json must contain exactly {} keys'.format(self.outputs))
                raise AssertionError
        
        elif self.output_check_mode == 'ignore':
            return

        else:
            print('output_check_mode should be one of ["filter","raise","ignore"]')
            raise ValueError

    @abstractmethod
    def fit(self,**inputs):
        pass

    @abstractmethod
    def transform(self,**inputs):
        pass


class Capsula():
    
    def __init__(
        self,
        estimator,
        name = None,
        fit_method = 'fit',
        fit_only = False,
        transform_method = 'transform',
        transform_only = False,
        estimator_fitargs = {},
        estimator_transformargs = {},
        required_inputs = {},
        optional_inputs = {},
        allowed_outputs = {},
        input_nodes = set({}),
        output_nodes = set({}),
        departures = [],
        landing_zone = {},
        takeoff_zone = {},
        is_fitted = False,
        is_transformed = False,
        is_callable = False,
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

        if not is_callable:
            if callable(estimator):
                is_callable = True
        if is_callable:
            transform_method = '__call__'
            fit_method = '__call__'

        if not name:
            try:
                name = estimator.__name__
            except:
                name = estimator.__class__.__name__


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

        output_nodes = set(output_nodes)
        input_nodes = set(input_nodes)

        ##### define everythong before this line
        local_vars = locals()
        for var_name in local_vars:            
            setattr(self, var_name,local_vars[var_name])

        # set states unsetable by __init__ args
        self.required_inputs_landed = False
        return

    def __str__(self):
        return self.name
    
    #def __repr__(self):
    #    return self.name
    
    def __call__(self, input_nodes):
        if not (isinstance(input_nodes,list) or isinstance(input_nodes,Capsula)):
            raise TypeError('input must be instance of Capsula or list o Capsulas')

        if isinstance(input_nodes,set):
            self.input_nodes = inputs
        else:
            self.input_nodes = set(input_nodes)
        return self

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
        print('{} stored in {} takeoffzone'.format(set(self.takeoff_zone),self.name))

    def take(self, variables, sender):
        
        inputs = sender.send(
            variables = variables,
            to_node = self.name,
        )
        landing_intersection = set(self.landing_zone).intersection(inputs)
        if landing_intersection:
            warnings.warn('an input colision occured in the landing zone with the following variables: {}. old values will be overwritten.'.format(landing_intersection))
        
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
                print('missing params in {} : '.format(self.name) + str(set(params) - set(self.landing_zone)))
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
        print(inputs)
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
            warnings.warn(('{} output type is {} instead of {}.\noutput have been wrapped in dict with key {}'.format(self.name, type(output), 'dict', str(self.output_name['transform']))))

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
            optional_inputs['transform'] = inspectobjs[1:]['transform'].args[
                                           -len(inspectobjs['transform'].defaults):]
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
                optional_inputs['transform'] = inspectobjs[1:]['transform'].args[
                                               -len(inspectobjs['transform'].defaults):]
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