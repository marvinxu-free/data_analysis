
import seaborn as sns
import itertools

def maxent_style(func):
    def maxent_style_inner(*args,**kwargs):
        print(func.__name__)
        with sns.axes_style('whitegrid',{"axes.grid":True,"axes.edgecolor":'white'}):
            sns.set_palette('muted',n_colors=20)
            kwargs['palette'] = itertools.cycle(sns.color_palette('muted'))
            func(*args,**kwargs)
    return maxent_style_inner

def remove_palette(func):
    def remove_palette_inner(*args,**kwargs):
        print(func.__name__)
        if kwargs.get('palette') is not None:
            kwargs.pop('palette')
        func(*args,**kwargs)
    return remove_palette_inner

