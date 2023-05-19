# WfCommons NextFlow workflow tracer

A tool to trace NextFlow workflow executions and produce WfFormat workflow instances.

## Modifications to NextFlow

A few modifications to the NextFlow source code were made in order to trace the inputs and outputs for each task in the workflow.
In particular, the file `TaskProcessor.groovy`, located in directory `/modules/nextflow/src/main/groovy/nextflow/processor/`, is modified. 
We reference the latest NextFlow release version, v22.10.7:
* https://github.com/nextflow-io/nextflow/blob/v22.10.7/modules/nextflow/src/main/groovy/nextflow/processor/TaskProcessor.groovy

1. In function 

```groovy
/**
 * Bind the expected output files to the corresponding output channels
 * @param processor
 */
synchronized protected void bindOutputs( TaskRun task ) {

    // -- creates the map of all tuple values to bind
    Map<Short,List> tuples = [:]
    for( OutParam param : config.getOutputs() ) {
        tuples.put(param.index, [])
    }

    // -- collects the values to bind
    for( OutParam param: task.outputs.keySet() ){
        def value = task.outputs.get(param)

        switch( param ) {
        case StdOutParam:
            log.trace "Process $name > normalize stdout param: $param"
            value = value instanceof Path ? value.text : value?.toString()

        case OptionalParam:
            if( !value && param instanceof OptionalParam && param.optional ) {
                final holder = [] as MissingParam; holder.missing = param
                tuples[param.index] = holder
                break
            }

        case EnvOutParam:
        case ValueOutParam:
            log.trace "Process $name > collecting out param: ${param} = $value"
            tuples[param.index].add(value)
            break

        default:
            throw new IllegalArgumentException("Illegal output parameter type: $param")
        }
    }

    // -- bind out the collected values
    for( OutParam param : config.getOutputs() ) {
        def list = tuples[param.index]
        if( list == null )
            throw new IllegalStateException()

        if( list instanceof MissingParam ) {
            log.debug "Process $name > Skipping output binding because one or more optional files are missing: $list.missing"
            continue
        }

        if( param.mode == BasicMode.standard ) {
            log.trace "Process $name > Binding out param: ${param} = ${list}"
            bindOutParam(param, list)
        }

        else if( param.mode == BasicMode.flatten ) {
            log.trace "Process $name > Flatting out param: ${param} = ${list}"
            CollectionHelper.flatten( list ) {
                bindOutParam( param, it )
            }
        }

        else if( param.mode == TupleOutParam.CombineMode.combine ) {
            log.trace "Process $name > Combining out param: ${param} = ${list}"
            final combs = (List<List>)list.combinations()
            for( def it : combs ) { bindOutParam(param, it) }
        }

        else
            throw new IllegalStateException("Unknown bind output parameter type: ${param}")
    }

    // -- finally prints out the task output when 'debug' is true
    if( task.config.debug ) {
        task.echoStdout(session)
    }
}
```
add `log.debug` statements in the `if` and `else if` statements in the last `for` loop, as follows:
```groovy
/**
 * Bind the expected output files to the corresponding output channels
 * @param processor
 */
synchronized protected void bindOutputs( TaskRun task ) {

    // -- creates the map of all tuple values to bind
    Map<Short,List> tuples = [:]
    for( OutParam param : config.getOutputs() ) {
        tuples.put(param.index, [])
    }

    // -- collects the values to bind
    for( OutParam param: task.outputs.keySet() ){
        def value = task.outputs.get(param)

        switch( param ) {
        case StdOutParam:
            log.trace "Process $name > normalize stdout param: $param"
            value = value instanceof Path ? value.text : value?.toString()

        case OptionalParam:
            if( !value && param instanceof OptionalParam && param.optional ) {
                final holder = [] as MissingParam; holder.missing = param
                tuples[param.index] = holder
                break
            }

        case EnvOutParam:
        case ValueOutParam:
            log.trace "Process $name > collecting out param: ${param} = $value"
            tuples[param.index].add(value)
            break

        default:
            throw new IllegalArgumentException("Illegal output parameter type: $param")
        }
    }

    // -- bind out the collected values
    for( OutParam param : config.getOutputs() ) {
        def list = tuples[param.index]
        if( list == null )
            throw new IllegalStateException()

        if( list instanceof MissingParam ) {
            log.debug "Process $name > Skipping output binding because one or more optional files are missing: $list.missing"
            continue
        }

        if( param.mode == BasicMode.standard ) {
            log.trace "Process $name > Binding out param: ${param} = ${list}"
            log.debug "Process $name ${task.id} > Binding out param: ${param} = ${list}"
            bindOutParam(param, list)
        }

        else if( param.mode == BasicMode.flatten ) {
            log.trace "Process $name > Flatting out param: ${param} = ${list}"
            CollectionHelper.flatten( list ) {
                log.debug "Process $name ${task.id} > Binding out param: ${param} = ${it}"
                bindOutParam( param, it )
            }
        }

        else if( param.mode == TupleOutParam.CombineMode.combine ) {
            log.trace "Process $name > Combining out param: ${param} = ${list}"
            final combs = (List<List>)list.combinations()
            for( def it : combs ) { 
                log.debug "Process $name ${task.id} > Binding out param: ${param} = ${it}"
                bindOutParam(param, it) 
            }
        }

        else
            throw new IllegalStateException("Unknown bind output parameter type: ${param}")
    }

    // -- finally prints out the task output when 'debug' is true
    if( task.config.debug ) {
        task.echoStdout(session)
    }
}

```

2. In function 
```groovy
@Override
List<Object> beforeRun(final DataflowProcessor processor, final List<Object> messages) {
    log.trace "<${name}> Before run -- messages: ${messages}"
    // the counter must be incremented here, otherwise it won't be consistent
    state.update { StateObj it -> it.incSubmitted() }
    // task index must be created here to guarantee consistent ordering
    // with the sequence of messages arrival since this method is executed in a thread safe manner
    final params = new TaskStartParams(TaskId.next(), indexCount.incrementAndGet())
    final result = new ArrayList(2)
    result[0] = params
    result[1] = messages
    return result
}
```
save the `TaskId.next()` and add a `log.debug` statement before the `return` statement, as follows:
```groovy
@Override
List<Object> beforeRun(final DataflowProcessor processor, final List<Object> messages) {
    log.trace "<${name}> Before run -- messages: ${messages}"
    // the counter must be incremented here, otherwise it won't be consistent
    state.update { StateObj it -> it.incSubmitted() }
    // task index must be created here to guarantee consistent ordering
    // with the sequence of messages arrival since this method is executed in a thread safe manner
    def id = TaskId.next()
    final params = new TaskStartParams(id, indexCount.incrementAndGet())
    final result = new ArrayList(2)
    result[0] = params
    result[1] = messages
    log.debug "<${name} ${id}> Before run -- messages: ${messages}"
    return result
}
```

### Build the modified NextFlow

Follow the NextFlow build from source instructions, i.e., `make compile`,
after which `./launch.sh` can be used while in the NextFlow root directory
to start the modified NextFlow.


## Collecting workflow instances 

The script `nf_to_wf.py` is used to both run the workflow using NextFlow and process log files to construct a JSON WfCommons workflow instance that complies with the WfFormat schema. 

This script requires the variable `nextflow_path` on line 320 to be set to the NextFlow build directory, for example:
```groovy
nextflow_path = "./nextflow-22.10.7/launch.sh"
```

The script takes 3 arguments: the workflow name, the working directory (where workflows can store files), and the JSON output file name.



This script operates as follows (it assumes the `trace_nextflow.config` and `template-scriptlog.txt` files are in the NextFlow root directory, however, they can be located elsewhere if desired): 

1. It runs the workflow as follows
```bash
./launch.sh -log <log file> run nf-core/<workflow> -profile test,docker -c trace_nextflow.config --outdir <workflow output directory> > <stdout file>
```
which creates a *log* file (via command line argument), a *trace* file (via `trace_nextflow.config`), a *dot* file (via `trace_nextflow.config`), and a *stdout* file (via bash command line).

2. It finds the run name for the executed workflow
```bash
./launch.sh log
```
(alternatively, it inspects the *stdout* file).

3. It gets the script commands for each task in the workflow
```bash
./launch.sh log <run name> -t template-scriptlog.txt > <scripts log file>
```
which creates a *scripts* file (via `template-scriptlog.txt`).

4. It parses the generated files to construct the WfCommons workflow instance.


## Notes

* It is assumed that on completion of the workflow (i.e., `workflow.onComplete`) the workflow prints the summary to stdout using `NfcoreTemplate.summary()`. To our knowledge, all nf-core workflows output this summary.

* Each task in the workflow requests a single CPU (via `trace_nextflow.config`).

* The *trace* and *dot* file locations are hard coded in `trace_nextflow.config` and are automatically overwritten after each execution.


