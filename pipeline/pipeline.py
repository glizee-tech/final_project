resources:
  jobs:
    my_pipeline:
      name: my-pipeline
      tasks:
        - task_key: Landing
          notebook_task:
            notebook_path: /Repos/herbelleau@gmail.com/final_project/notebook/landing
            source: WORKSPACE
        - task_key: Bronze
          depends_on:
            - task_key: Landing
          notebook_task:
            notebook_path: /Repos/herbelleau@gmail.com/final_project/notebook/bronze
            source: WORKSPACE
          environment_key: Default
        - task_key: Silver_suppliers
          depends_on:
            - task_key: Bronze
          notebook_task:
            notebook_path: /Repos/herbelleau@gmail.com/final_project/notebook/silver_suppliers
            source: WORKSPACE
          environment_key: Default
        - task_key: Silver_orders
          depends_on:
            - task_key: Silver_suppliers
          notebook_task:
            notebook_path: /Repos/herbelleau@gmail.com/final_project/notebook/silver_orders
            source: WORKSPACE
          environment_key: Default
        - task_key: Silver_incidents
          depends_on:
            - task_key: Silver_orders
          notebook_task:
            notebook_path: /Repos/herbelleau@gmail.com/final_project/notebook/silver_incidents
            source: WORKSPACE
          environment_key: Default
      git_source:
        git_url: https://github.com/glizee-tech/final_project.git
        git_provider: gitHub
        git_branch: master
      queue:
        enabled: true
      environments:
        - environment_key: Default
          spec:
            environment_version: "4"
      performance_target: PERFORMANCE_OPTIMIZED
