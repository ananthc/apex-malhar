package org.apache.apex.malhar.python.base.partitioner;

import java.util.Collection;
import java.util.Map;

import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;

import com.datatorrent.api.Partitioner;

public abstract class AbstractPythonExecutionPartitioner implements Partitioner<BasePythonExecutionOperator>
{
  @Override
  public Collection<Partition<BasePythonExecutionOperator>> definePartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    return null;
  }

  @Override
  public void partitioned(Map<Integer, Partition<BasePythonExecutionOperator>> partitions)
  {

  }
}
