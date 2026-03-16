resources:
  jobs:
    Pipeline:
      name: Pipeline
      tasks:
        - task_key: Setup
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/setup
            source: WORKSPACE
        - task_key: Landing
          depends_on:
            - task_key: Setup
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/landing
            source: WORKSPACE
          environment_key: Default
        - task_key: Bronze
          depends_on:
            - task_key: Landing
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/bronze
            source: WORKSPACE
        - task_key: Silver_suppliers
          depends_on:
            - task_key: Bronze
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/silver_suppliers
            source: WORKSPACE
        - task_key: silver_orders
          depends_on:
            - task_key: Silver_suppliers
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/silver_incidents
            source: WORKSPACE
        - task_key: silver_incidents
          depends_on:
            - task_key: silver_orders
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/gold
            source: WORKSPACE
        - task_key: gold
          depends_on:
            - task_key: silver_incidents
          notebook_task:
            notebook_path: /Workspace/Users/ghislainlizee@gmail.com/final_project/notebook/gold
            source: WORKSPACE
      queue:
        enabled: true
      environments:
        - environment_key: Default
          spec:
            environment_version: "4"
      performance_target: PERFORMANCE_OPTIMIZED
