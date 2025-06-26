"# dld-api" 
# DesInventar Sendai API
## Introduction
The DesInventar system lets users register detailed information for daily losses.

## How to run
To start the service: 
> doc/disapi.sh start
The default port is 8081, but it's possible to use a different one. Please modified the script disapi.sh and change PORT 8081 to another one.
To stop the service:
> doc/disapi.sh Stop
To incorporate the program when Linux start, please follow:
> sudo crontab -e
> add this line adding the correct path to disapi.sh script: @reboot /path/disapi.sh start
> save and restart

## Parameters
The URL to access is: http://localhost:8081/consolidated?country=pan&year=2018&indicator=a2a

>country: lowercase string 3 iso code for the country required to get the consolidated value;
>year: numeric value for the year to get the consolidated value;
>indicator: lowercase string to refer to the Sendai indicator desired: only indicators of Targets A, B, C and D.

Version 2.3X provide a way to communicate with the datacards and the structure of DesInventar System:
- api/geographylist?country=cri
- api/hazardslist?country=idn
- api/effectslist?country=mwi
- api/datacards?country=lka&page=0

datacards endpoint returns 1000 datacards per page, then go to the next page.


# Messages
The result of this service is:
> a JSON format with this generic structure:
```
{"ctycode":"ISO","year":"YYYY","indicator":"sfm","value": {LIST}, "source": "...", "hazards": {LIST}, "subdivision1": {LIST}, "otherdisaggregation": {LIST} }
The LIST varies depending on the Country and Indicator.
```
> an empty response when: the country, the year or the indicator are not available, because there is not information in DesInventar 

# Examples
```
{"ctycode":"pan","year":"2017","indicator":"a2a","value":{"total":27},"source":"DesInventar Official database","hazards":{"Ahogamiento":{"total":15},"Colapso estructural":{"total":0},"Alud":{"total":1},"Inundación":{"total":0},"Deslizamiento":{"total":0},"Otro":{"total":0},"Vendaval/Vientos Fuertes":{"total":0},"Accidente":{"total":11},"Caída de Arbol":{"total":0},"Epidemia":{"total":0},"Tormenta eléctrica":{"total":0},"Lluvias":{"total":0},"Incendio":{"total":0},"Actividad Volcánica":{"total":0}},"subdivision1":{"PANAMA":{"total":1},"COCLE":{"total":12},"LOS SANTOS":{"total":0},"BOCAS DEL TORO":{"total":6},"COLON":{"total":0},"VERAGUAS":{"total":7},"CHIRIQUI":{"total":0},"HERRERA":{"total":1}},"otherdisaggregation":{"Disability":{},"Sex":{},"Income":{},"Age":{}}}
```
```
{"ctycode":"vut","year":"2017","indicator":"b2","value":{"total":0},"source":"DesInventar Official database","hazards":{},"subdivision1":{},"otherdisaggregation":{"Disability":{"Persons with disability":{"total":0}},"Sex":{"men":{"total":0},"women":{"total":0}},"Income":{"Under national poverty line":{"total":0}},"Age":{"Children (0-14)":{"total":0},"Seniors (65 +)":{"total":0},"Adults (15-64)":{"total":0}}}}
```
