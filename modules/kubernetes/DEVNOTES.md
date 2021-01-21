Building and testing Kubernetes module
=========================================

The instructions provide a guidance on how to build and test Ignite Kubernetes IP finder in Kubernetes environment.

To test the IP finder you have to build the whole Apache Ignite project, package the binary as a Docker image and
feed the image to your kubernetes environment.

Building Apache Ignite
=========================

To assemble an Apache Ignite binary use instructions from DEVNOTES.txt file in the root of the repository.

Note, if you alter the build instruction somehow make sure to update the files under 'config' folder if needed.

Installing Docker and Minikube
==============================

Install Docker and Minikube for testing purpose in your development environment.

Once this is done, make sure that Minikube sees Docker images registered locally:
    eval $(minikube docker-env)

Start Minikube:
    minikube start

Assembling Apache Ignite Docker Image
=====================================

Create a folder for all the files to be placed in the Docker image and copy the following there:
    - Apache Ignite binary in a zip form built at the step above.
    - Dockerfile from `ignite-kubernetes/config/Dockerfile`.
    - Ignite configuration with enabled Kubernetes IP finder from `ignite-kubernetes/config/example-kube.xml`.
    - The executable file that starts the Ignite node process from `ignite-kubernetes/config/run.sh`

To prepare the image, navigate to the folder and execute the following command:
    docker build -t ignite-kube:v1 --build-arg IGNITE_VERSION=2.10.0-SNAPSHOT .

Creating containerized Ignite pods and Ignite lookup service
============================================================

Create the Kuberenetes role that provides access to endpoints of pods to provide communication between Ignite nodes:
    kubectl apply -f {path_to}/ignite-account-role.yaml

Create the Kubernetes service account to bind the role:
    kubectl apply -f {path_to}/ignite-service-account.yaml
    kubectl apply -f {path_to}/ignite-role-binding.yaml

Start the Kubernetes service that is used for IP addresses lookup. Use `ignite-kubernetes/config/ignite-service.yaml`:
	kubectl create -f {path_to}/ignite-service.yaml

Create and deploy Ignite pods using `ignite-kubernetes/config/ignite-deployment.yaml` configuration:
    kubectl create -f {path_to}/ignite-deployment.yaml

Make sure that the pods were deployed and running properly:
    kubectl get pod
    kubectl logs {pod name}

Increase or decrease number of Ignite pods checking that Kubernetes IP finder works as expected:
    kubectl scale --replicas=4 -f {path_to}/ignite-deployment.yaml

Docker Image Redeployment
=========================

If you need to redeploy the docker image after it gets updated and you prefer not to change the image version then
delete a current Kubernetes Ignite deployment (don't delete the service):
    kubectl delete deployment ignite-cluster

After that you are free to build and deploy an updated docker image using the same commands as listed above.
