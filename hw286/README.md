# CS286 Homework, Fall 2017: SQL Query Optimizer in SQL
In this assignment, you will build a SQL optimizer in SQL. This is an example of a [self-hosting compiler](https://en.wikipedia.org/wiki/History_of_compiler_construction#Self-hosting_compilers).

We have abstracted away some of the fussy details for you:
  - Input: We are not expecting you to parse an SQL query; we assume the query is already described as a set of relational metadata.
  - Output: We are not expecting you to translate the symbolic query plan into something that can be executed; your chosen plan will be represented via a set of relational metadata that can be parsed into a physical query plan for a query executor.
    
We have given you the [basic schema for the optimizer state](img/metaschema.png), though you may choose to extend it with any views and supplemental tables you like.

Your job will be to:
  1. Write queries in SQL to populate the summary statistics for the optimizer.
  2. Complete the design of a schema to hold the state of Selinger's dynamic programming algorithm.
  3. Implement Selinger's dynamic programming algorithm using SQL
  4. Make sure your output is in a target schema that can be visualized in the provided [Vega3](https://github.com/vega/vega) visualization spec.
    
## Setup: Getting Started

You should only have to run the commands in this README once.

### Install Pip, Jupyter Notebook support on your VM
Our class VM does not include the Python libraries we need to run Jupyter notebooks, so we will install them by hand.  

To get `pip` and `jupyter` installed, please follow the instructions at [this website](https://www.digitalocean.com/community/tutorials/how-to-set-up-a-jupyter-notebook-to-run-ipython-on-ubuntu-16-04), Steps 1-3.

### Install Postgres support for Jupyter on your VM
In a bash shell with your favorite Python environment loaded, add the `psycopg2` package to make connections to postgres, and the very handy [`ipython-sql`] package (https://github.com/catherinedevlin/ipython-sql) to integrate SQL queries into Jupyter notebooks.

```bash
% sudo pip install psycopg2
% sudo pip install ipython-sql
```

You will want to refer to the `ipython-sql` documentation for information on:
- How to create a notebook cell that is all in SQL, using the `%%sql` heading.
- How to embed a line of SQL within python using the `%sql` line header.
- How to pass variables into a `%sql` line using the `$` and `:` prefixes.

### Set up Jupyter support for Vega 3
In the same bash shell, we add support for the latest version of the [Vega](https://github.com/vega/vega) visualization library so we can see our plan trees in the notebook.  (Thanks to [@domoritz](https://www.domoritz.de/) for releasing this to us early!)

```bash
% sudo pip install pandas vega3
% sudo pip install --upgrade notebook  # need jupyter_client >= 4.2 for sys-prefix below
% sudo jupyter nbextension install --sys-prefix --py vega3
% sudo jupyter nbextension enable vega3 --py --sys-prefix
```

## The Notebook
You should not be ready to do the rest of the homework, which is in [this Jupyter notebook](sqloptimizer.ipynb).  You can view it on github, but to interact with it you need to get back into the VM and run jupyter:

```bash
% jupyter notebook sqloptimizer.ipynb
```

## Working with psql
While debugging, you may want to connect to your database using psql rather than the jupyter notebook. If so, you can connect like this:
```bash
% psql -d optimizer
psql (9.6.5)
Type "help" for help.

optimizer=#
```
Or equivalently, you can switch databases while inside of psql using the `\c` command:
```
% psql
psql (9.6.5)
Type "help" for help.

vagrant=# \c optimizer
You are now connected to database "optimizer" as user "jmh".
optimizer=# 
```

# Testing and Grades
As this is a graduate assignment, you are expected to develop your own tests. You can consider unit-testing your cost and selectivity formulae. You may want to try a wider variety of different schemas and queries. You can also disable various join methods or costs and see how that affects your outputs.

We will grade this assignment *qualitatively* with a letter grade. Selecting `Run All` from the `Cell` menu of the notebook must produce a good plan for the query given in the notebook, with reasonable cost and size estimates. We may also try some other queries, and see how your chosen plans and cost estimates compare to ours. We may also read over your code.

## Extra credit: incremental visualization
The [visualization notebook](visualizer.ipynb) included provides some Vega3 code for visualizing a single plan tree. It would be much more useful to be able to visualize all the subtrees searched during dynamic programming. One obvious improvement would be to use Vega's [`group` marks](https://vega.github.io/vega/docs/marks/group/) to show many subgraphs in a grid, something like in [this example visualization](https://vega.github.io/vega/examples/brushing-scatter-plots/).

Extra credit of 5% *for the semester final grade* is available if you visualize the plan space in ways that could be useful to future students in understanding query optimization.