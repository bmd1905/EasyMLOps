FROM rayproject/ray:2.9.0-py39

# No need for additional installations since the base image has everything we need
CMD ["bash", "-c", "ray start --head --num-cpus 1 --dashboard-host 0.0.0.0 --include-dashboard true --block"]
