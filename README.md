# Apache-Beam-Experiments

For Cloud execution, specify DataflowRunner and set the Cloud Platform
project, job name, temporary files location, and region.

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(
    flags=argv,
    runner='DataflowRunner',
    project='my-project-id',
    job_name='unique-job-name',
    temp_location='gs://my-bucket/temp',
    region='Region')
```
Create the Pipeline with the specified options.

```python
with beam.Pipeline(options=options) as pipeline:
```
