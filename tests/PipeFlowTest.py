from Nodes import Inputer
from Base import Capsula
from Pipeline import Custom
from Utils import model_to_dot


# define estimator objects
a = 1
b = 2
c = 3

{'input1': a}


{'input1': b, 'input2': 2 * b}


{'externalinput': c}

def foo(input1):
    return {'outputfoo': input1 ** 2}

def bar(input1, input2):
    return {'outputbar': input1 + input2}

def foobar(outputfoo, outputbar):
    return {'outputfoobar': outputfoo + outputbar}

def pipeoutput(outputfoobar, externalinput):
    return outputfoobar + externalinput

# create nodes
inputer_foo = Inputer(infoo, name='inputer_foo')
inputer_bar = Inputer(inbar, name='inputer_bar')
inputer_externalinput = Inputer(externalinput, name='inputer_externalinput')

node_foo = Capsula(foo, name='foo')
node_bar = Capsula(bar, name='bar')

node_foobar = Capsula(foobar, name='foobar')
node_pipeoutput = Capsula(pipeoutput, name='output_node')
# connect nodes - the call parameters are the input nodes
node_foo(inputer_foo)
node_bar(inputer_bar)
node_foobar([node_foo, node_bar])
node_pipeoutput([inputer_externalinput, node_foobar])

# build pipeline
pipe = Custom(input_nodes=[inputer_foo, inputer_bar, inputer_externalinput], output_nodes=[node_pipeoutput])

# fit pipeline
pipe.fit()
# transform pipeline
output = pipe.transform()
print('output = {}'.format(output))

# draw graph
pipe.plot_graph(with_labels=True)

pipe.plot_pipeline(r'pipeline.png')
# save pipeline
pipe.save(r'pipe_test.sav')
#load pipeline object
pipe_load = Custom.load(r'pipe_test.sav')
#transform objects
loaded_outputs = pipe_load.transform()
outputs = pipe.transform()
#check outputs
print(loaded_outputs == outputs)
