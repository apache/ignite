#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ver=2.8.1

if [ $# -eq 0 ]
  then
    echo "Use one input parameter: stateless or stateful"
	exit 1
fi

mode=$1

kubectl delete configmap ignite-config -n ignite --ignore-not-found

if [ "$mode" = "stateful" ]; then
kubectl delete deployment ignite-cluster -n ignite --ignore-not-found
else 
kubectl delete statefulset ignite-cluster -n ignite --ignore-not-found
fi

kubectl delete service ignite-service -n ignite --ignore-not-found

kubectl delete clusterrole ignite -n ignite --ignore-not-found

kubectl delete clusterrolebinding ignite -n ignite --ignore-not-found

kubectl delete namespace ignite --ignore-not-found


# tag::create-namespace[]
kubectl create namespace ignite
# end::create-namespace[]

# tag::create-service[]
kubectl create -f service.yaml
# end::create-service[]


# tag::create-service-account[]
kubectl create sa ignite -n ignite
# end::create-service-account[]

# tag::create-cluster-role[]
kubectl create -f cluster-role.yaml
# end::create-cluster-role[]


if [ "$mode" = "stateful" ]; then
	cd stateful
    sed -e "s/{version}/$ver/" statefulset-template.yaml > statefulset.yaml
else 
   cd stateless
   sed -e "s/{version}/$ver/" deployment-template.yaml > deployment.yaml
fi

# tag::create-configmap[]
kubectl create configmap ignite-config -n ignite --from-file=node-configuration.xml
# end::create-configmap[]

if [ "$mode" = "stateful" ]; then

  # tag::create-statefulset[]
  kubectl create -f statefulset.yaml
  # end::create-statefulset[]
    rm statefulset.yaml

else 

  # tag::create-deployment[]
  kubectl create -f deployment.yaml
  # end::create-deployment[]
  rm deployment.yaml

fi


# tag::get-pods[]
kubectl get pods -n ignite
# end::get-pods[]
