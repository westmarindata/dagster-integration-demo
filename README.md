## Dagster Integration Demo

There are two projects here. The `demo_integration` folder contains a Dagster Integration
which serves as an example of how you might create a simple Dagster Integration that 
connects to an API and runs an arbitrary job.

The `dagster-project` repo contains an example of how your user would use the `demo-integration` 
to materialize an asset.


### Clone the repo

```
git clone https://github.com/westmarindata/dagster-integration-demo.git
cd dagster-integration
```

Create a virtual env using one of fifteen possible methods.
Here's mine using pyenv:

```
pyenv virtualenv 3.10.6 dagster-demo
pyenv local dagster-demo
```

Install dependencies and run dagster

```
make run
```

Visit http://localhost:3000 and materialize the asset to see it run!