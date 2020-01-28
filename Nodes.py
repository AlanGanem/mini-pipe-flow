from .Base import Capsula
import inspect
import warnings

class Node(Capsula):
	'''
	rename the capsula class to avoid calling from Base module
	'''
	pass

class Inputer(Capsula):
	def __init__(self,inputs,**kwargs):
		if not inputs.__class__ in [list,set,tuple]:
			raise TypeError('inputs must be instance of [list,set,tuple], not {}'.format(inputs.__class__.__name__))
		for input_name in inputs:
			non_str_inputs = []
			if input_name.__class__ != str:
				non_str_inputs.append(input_name)

		if non_str_inputs:
			raise TypeError('all input names in input must be str. {} are not'.format(non_str_inputs))
		inputs = set(inputs)

		super().__init__(
			estimator = None,
			required_inputs = {'fit':inputs,'transform':inputs},
			**kwargs
			)
		self.is_fitted = True

	#def __call__(self,inputs):
	#	if not isinstance(inputs,dict):
	#		raise TypeError('Inputs must be a dict')
	#	self.store(inputs)

	def transform(self,):
		self.is_transformed = True
		return self.takeoff_zone
    
	def fit(self):
		self.transform()
		self.is_fitted = True
		return

class Renamer(Capsula):
	'''
	renames the output dict keys.
	mapper renames from key to value
	'''
	def __init__(self,mapper, **nodeargs):
		estimator = self.renamer
		super().__init__(
			estimator=estimator,			
            required_inputs = {'fit':list(mapper.keys()),'transform':list(mapper.keys())},
			optional_inputs= {'fit': list(mapper.keys()), 'transform': list(mapper.keys())},
			filter_inputs = False,
			**nodeargs
		)
        
		self.set_fitargs(mapper = mapper)
		self.set_transformargs(mapper = mapper)
		self.mapper = mapper

	def take(self, variables, sender):
		variables = 'all'
		inputs = sender.send(
			variables=variables,
			to_node=self.name,
		)
		landing_intersection = set(self.landing_zone).intersection(inputs)
		if landing_intersection:
			warnings.warn('an input colision occured in the landing zone with the following variables: {}'.format(
				landing_intersection))


		self.landing_zone = {**self.landing_zone, **inputs}


	def renamer(self, mapper, **inputs):

		assert isinstance(mapper, dict)
		for key in mapper:
			try:
				inputs[mapper[key]] = inputs.pop(key)
			except KeyError:
				pass
		outputs_dict = inputs
		return outputs_dict

class Getter(Capsula):
	def __init__(self,attributes,**nodeargs):
		assert isinstance(attributes, list)
		assert all([isinstance(i, str) for i in attributes])
		required_inputs = {
			'fit': attributes,
			'transform': attributes
		}
		super().__init__(
			estimator = None,
			required_inputs = required_inputs,
			none_estim_outputs_fit = attributes,
        	none_estim_outputs_transform = attributes,
			**nodeargs
		)
		self.attributes = attributes


	def take(self, variables ,sender):
		attributes = self.attributes
		estimator = sender.hatch()
		self.landing_zone = {}
		for attribute in attributes:
			try:
				attribute_value = getattr(estimator, attribute)
				self.landing_zone = {**self.landing_zone, **{attribute:attribute_value}}
			except:
				print('{} does not have the attribute {}'.format(sender, attribute))
				pass


	def fit(self):
		self.bypass()
		self.is_fitted = True
	def transform(self):
		self.bypass()
		self.is_transformed = True


class Debugger(Capsula):
	pass