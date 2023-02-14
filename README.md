# nextflow_workflow_tracer
A tool to trace nextflow workflow executions and produce WfFormat workflow instances

## Modifications to Nextflow

We were unable to find a way to enable trace level logging in Nextflow, hence, small modifications to the source code were made in order to trace all necessary information.
In particular, the file `TaskProcessor.groovy`, located in directory `/modules/nextflow/src/main/groovy/nextflow/processor/`, is modified. 
We reference the following version:
* https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/processor/TaskProcessor.groovy
* commit: a5df62f6efffc24e0395ef05e9a43461174b453c

1. In function
```groovy
protected void bindOutputs0(Map<Short,List> tuples) {
        // -- bind out the collected values
        for( OutParam param : config.getOutputs() ) {
            final outValue = tuples[param.index]
            if( outValue == null )
                throw new IllegalStateException()

            if( outValue instanceof MissingParam ) {
                log.debug "Process $name > Skipping output binding because one or more optional files are missing: $outValue.missing"
                continue
            }

            log.trace "Process $name > Binding out param: ${param} = ${outValue}"
            bindOutParam(param, outValue)
        }
    }
````
(starting on line 1415), modify line 1427
```groovy
log.trace "Process $name > Binding out param: ${param} = ${outValue}"
```
to output to `log.debug`
```groovy
log.debug "Process $name > Binding out param: ${param} = ${outValue}"
```

2. In function 
```groovy
@Override
        Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            if( log.isTraceEnabled() ) {
                def channelName = config.getInputs()?.names?.get(index)
                def taskName = currentTask.get()?.name ?: name
                log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
            }

            super.messageArrived(processor, channel, index, message)
        }
```
(starting on line 2393), Remove the `if` statement on line 2394 (and its corresponding ending bracket on line 2398) and modify line 2397 
```groovy
log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
```
to output to `log.debug`
```groovy
log.debug "<${taskName}> Message arrived -- ${channelName} => ${message}"
```
### Build and Run the modified Nextflow

Follow the Nextflow build from source instructions, i.e., `make compile`, after which `./launch.sh` can be used while in the build directory to start the modified Nextflow.
