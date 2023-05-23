# Parallel Qualitative Reasoner (ParQR-QE)
ParQR-QE is a qualitative spatial reasoner designed to answer GeoSPARQL queries over large scale knowledge graphs e.g. YAGO, DBPedia. There are two parts to ParQR-QE:
* A knowledge graph generator, see the package ```kggenerator```.
* The query engine, see the package ```queryengine```.

This repository also contains an example dataset. This is comprised of a subset of the YAGO knowledge graph (./data/input/YAGO), and a subset of the GADM dataset (./data/inpt/GADM). The following provides a workflow for generating a knowledge graph using these inputs that can support GeoSPARQL queries. This is followed by examples of running queries. 

## Knowledge Graph Generator

### ParseYAGO (kggenerator.parsers.ParseYAGO)
Parses .nt files, generating parquet tables for each separate predicate.
* Input
	- ./data/input/YAGO/yago-sample.nt
* Output
	- ./data/enhanced-kg/YAGO-VP

### GeoJSONParser (kggenerator.parsers.GeoJSONParser)
Parses input GeoJSON and generates parquet tables (one row per polygon)
* Inputs
	- ./data/input/GADM/layer0.json
* Output
	- ./data/temp/gadm/layer0

### ConvertPointsToGeoSpark (kggenerator.ConvertPointsToGeoSpark)
Converts YAGO points to Sedona points
* Inputs
	- ./data/enhanced-kg/YAGO-VP/schema.org/geo
* Output
	- ./data/temp/points

### GenerateMBBs (kggenerator.GenerateMBBs)
Generates MBBs from the GADM geometries. This is needed later when matching GADM region to YAGO points.
* Inputs
	- ./data/temp/gadm/layer0
* Output
	- ./data/temp/mbbs/layer0

### PointInMBB (kggenerator.PointInMBB)
Identify which MBBs cover YAGO points.
* Inputs
	- ./data/temp/points 
	- ./data/temp/mbbs/layer0 
* Output
	- ./data/temp/pointInMBB/layer0

### LinkYAGOtoGADM0 (kggenerator.LinkYAGOtoGADM0)
Matches GADM Layer 1 regions to YAGO points
* Inputs
	- ./data/temp/gadm/layer0 
	- ./data/enhanced-kg/YAGO-VP/www.w3.org/label 
	- ./data/enhanced-kg/YAGO-VP/www.w3.org/type 
	- ./data/enhanced-kg/YAGO-VP/schema.org/dissolutionDate 
	- http://schema.org/Country 
	- ./data/temp/pointInMBB/layer0
* Output
	- ./data/temp/matches/layer0

### LinkYAGOtoGADM1 (kggenerator.LinkYAGOtoGADM1)
Matches GADM Layer 1 regions to YAGO points
* Inputs
	- ./data/input/gadm/layer1.json
	- ./data/temp/pointInMBB/layer1
	- ./data/enhanced-kg/YAGO-VP/www.w3.org/label
	- ./data/enhanced-kg/YAGO-VP/www.w3.org/type
	- ./data/enhanced-kg/YAGO-VP/schema.org/dissolutionDate
	- http://schema.org/AdministrativeArea
	- ./data/temp/matches/layer0
* Outputs
	- ./data/temp/matches/layer1

### ComputeEC (kggenerator.ComputeEC)
Computes EC relations betwen regions in a layer
* Inputs
	- ./data/temp/GADM/layer0 
	-  -45.0#90.0#90.0#0.0
* Output
 	- ./data/temp/ec/layer0

### PointInPolygon (kggenerator.PointInPolygon)
Runs point in polygon tests to find the GADM region each YAGO point is contained within
* Inputs
	- ./data/temp/points 
	- ./data/temp/gadm/layer1 
	- ./data/temp/matches/
* Output
	- ./data/temp/point-in-polygon

This is done for Layer 1 first. This then needs to be run a second time for Layer 0 using the unplaced points. 

### GenerateRCC8Network (kggenerator.GenerateRCC8Network)
Generates an RCC-8 network based on the above. It combines the results of the point in polygon tests and the EC relations computation with RCC-8 relations generated from ```containedInPlace``` triples and GADM region identifiers.
* Inputs
	- ./data/temp/point-in-polygon
	- ./data/temp/points
	- ./data/temp/ec/
	- ./data/temp/gadm/layer1
	- ./data/enhanced-kg/YAGO-VP/schema.org/containedInPlace
	- ./data/temp/matches/
	- 8 
* Outputs
	- ./data/enhanced-kg/rcc8-network

### DictionaryEncoder (kggenerator.DictionaryEncoder)
Dictionary encodes the RCC-8 network
* Inputs
	- ./data/enhanced-kg/rcc8-network/txt/rcc8.txt
	- ./data/enhanced-kg/rcc8-network/dictionary
	- 1
* Outputs
	- ./data/enhanced-kg/rcc8-network/encoded

### GenerateQuanData (kggenerator.GenerateQuanData)
Generates a single parquet table containing the geometries from GADM and the YAGO points. 
* Inputs
	- ./data/temp/gadm/
	- ./data/temp/points/
	- ./data/temp/matches/
* Outputs
	- ./data/enhanced-kg/geometries

## Running Queries

### Run Qualitative Queries
Execute queries using QSR
* Inputs
	- ./data/enhanced-kg/rcc8-network/encoded/rcc8.txt
	- ./data/enhanced-kg/rcc8-network/dictionary/
	- ./data/config/rcc8.calculus
	- ./data/config/predicates.txt
	- ./data/enhanced-kg/YAGO-VP/
	- ./data/config/queries/queries.txt
	- 8
### Run Range Queries
Execute queries quantitatively first, and then using QSR
* Inputs
	- ./data/enhanced-kg/rcc8-network/encoded/rcc8.txt
	- ./data/enhanced-kg/rcc8-network/dictionary/
	- ./data/config/rcc8.calculus
	- ./data/enhanced-kg/geometries
	- ./data/config/predicates.txt
	- ./data/enhanced-kg/YAGO-VP/
	- ./data/config/queries/range-queries.txt
	- hybrid
	- 8




