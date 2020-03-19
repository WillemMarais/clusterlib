# Notes
I want to refactor the picklejob module. Currently it depends on the module executor. Code written with
the module executor is too complicated and hard to understand. The hope is that the refactored picklejob module will
allow for code that is much more readable.

## Thoughts
1. There should be a picklejob workflow (computation graph) which should be exportable to JSON and easily imported.
2. Each pickle job that is executed receives 1) the pickle object that will be called as a function, 2) an input
parameter file, 3) the unique name of the job. The unique name of the job is used to get the required parameters from
the input parameter file.
3. From the workflow (computation graph) it MUST be easy to generate configuration code that allows exectution via
Makeflow, AWS or local processing.

## Ieas
1. Create an initial version of the computational graph. A graph is a dictionary where each entry consists of a
    1. A unique identifier.
    2. File path to the pickle object that will be called.
    3. The input parameters for the object, which may contain native python types and PickleVariables.
    4. An optional file path.
2. Once the initial computational graph has been created, the dependencies between the jobs are computed:

    For every entry that does not have a dependency entry, insert a dependency entry by checking the PickleVariables.

3. (Future) Check of the circular dependencies.

Each PickleVariable either has a unique name; if no name is provided, the name is replaced with a unique code. With the
final computational graph the makeflow file is created.
