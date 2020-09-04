from load_balancing import RoundRobinLoadBalancer, UtilizationAwareLoadBalancer

autoscale_warmup_time = 10
cpu_scale_factor = 5
load_balancer = RoundRobinLoadBalancer
