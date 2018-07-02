import matplotlib.pyplot as plt
import pandas as pd
from Pic.maxent_style import maxent_style,remove_palette
from Pic.output_table_macdown import print_macdown_table,print_table
from itertools import combinations

@remove_palette
@maxent_style
def makeHist(col,df,dpi=600,path=None,palette=None):
    """
    """
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 2, 1)
    maxValue = int(df[col].max())
    if maxValue > 10:
        step = int(maxValue / 10)
        bins = range(-step, maxValue, step)
        bins.append(maxValue)
    else:
        bins = range(-2, 10, 2)
    re_cols = pd.cut(df[col], bins)
    print('re_cols\n',re_cols)
    re = re_cols.value_counts(sort=False)
    print('re\n',re)
    re_div= re.div(re.sum())
    print('re_idv\n',re_div)
    re_div.plot.bar(ax=ax)
    ax.set_title(col)
    ax1 = fig.add_subplot(1, 2, 2)
    re.plot.bar(ax=ax1)
    ax1.set_title(col)
    ax1.set_yscale("log")
    fig.canvas.set_window_title(col)
    path +='/{0}'.format(col)+'.png'
    fig.savefig(filename=path,dpi=dpi,format='png')
    plt.show(block=False)


@maxent_style
@remove_palette
def anormalyScoreHist(cols,score,df,dpi=600,path=None,name="anormalyScore"):
    """
    """
    fig = plt.figure()
    ax = fig.add_subplot(1, 2, 1)
    maxValue = int(df[score].max())
    if maxValue > 10:
        step = int(maxValue / 10)
        bins = range(-step, maxValue, step)
        bins.append(maxValue)
    else:
        bins = range(-2, 10, 2)
    # re = pd.cut(df[cols], bins).value_counts(sort=False)
    re = pd.cut(df[score], bins=bins)
    # print('re\n',re.value_counts())
    re_group = df.groupby(re)[cols].agg('sum')
    print('re_group\n',re_group)
    re_group.plot.bar(ax=ax,stacked=True,legend=False)
    ax.set_title(name)
    # re_group[cols] =re_group[cols].apply(lambda x: 0 if x.any() <=0 or x.any() == None else np.log(x))
    ax1 = fig.add_subplot(1, 2, 2)
    re_group.plot.bar(ax=ax1,stacked=True,legend=False)
    patches, labels = ax1.get_legend_handles_labels()
    lgd=ax1.legend(patches, labels,loc='center left',ncol=3,bbox_to_anchor=(0.8,0.6),\
               fontsize='x-small',handlelength=0.5,handletextpad=0.8)
    ax1.set_title(name+' Log View')
    ax1.set_yscale("log")
    fig.canvas.set_window_title(name)
    path +='/{0}'.format(name)+'.png'
    fig.savefig(filename=path,dpi=dpi,format='png',bbox_extra_artists=(lgd,), bbox_inches='tight')
    plt.show(block=False)
    # plt.show()

@maxent_style
@remove_palette
def dataCorr(cols,df,dpi=600,title='data correlations',path=None,filter_value=1.0):
    corr_cols = combinations(cols,2)
    frames = []
    for corr_col in corr_cols:
        # print(tabulate(df.loc[(df[corr_col[0]] != 1) | (df[corr_col[1]] != 1),corr_col].corr(),showindex="always",)
        #                tablefmt='fancy_grid',headers=corr_col)
        frames.append(df.loc[(df[corr_col[0]] != filter_value) | (df[corr_col[1]] != filter_value),corr_col].corr())

    corr_result = pd.concat(frames)
    print(corr_result.shape ,'\n',corr_result)
    print_table(corr_result)
    grouped_result = corr_result.groupby(corr_result.index)
    print(grouped_result)
    agg_result = grouped_result.agg('sum')
    agg_result[agg_result >= 1] = 1
    print(agg_result)
    print_table(agg_result)

    print_macdown_table(agg_result)

    fig = plt.figure()
    ax = fig.add_subplot(1, 2, 1)
    agg_result.plot.bar(ax=ax,legend=False,width=0.9)
    patches, labels = ax.get_legend_handles_labels()
    ax.legend(patches, labels,loc='center left',bbox_to_anchor=(1.02,0.8))
    ax.set_xticks([])
    ax.set_title(title)
    fig.canvas.set_window_title(title)
    path +='/{0}'.format(title)+'.png'
    fig.savefig(filename=path,dpi=dpi,format='png')
    # plt.show()
    plt.show(block=False)

