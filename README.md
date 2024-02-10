# Dark Web Exploratory Analysis

## Overview
This repository presents an exploratory data analysis of a dataset sourced from the dark web. It includes not only the code used for the analysis but also the results of it, The aim is to uncover patterns, trends, and insights within this dataset.

## Dataset Description 
Data for processing is a JSON file containing 85291 documents with each document represented as
a JSON object with the following structure:
1. ”url”: the URL of the current HTML page.
2. ”parent”: the URL of the HTML page that contains the link (URL) to the current document.
3. ” HTML text”: the processed text of the HTML page.
4. ”category”: the category of the document based on the keywords found in the HTML text.
5. ”keywords”: the predefined keywords that were found in the HTML text.

## Repository Structure
- Component_count/: Directory containing heatmap representation and CSV file of the number of items each component contains 
- Components/: 1. Directory containing a folder for each component with a  table with the results of the PageRank on the specific component
  2. HeatMap representation (top keywords vs top domains )
  3. Folder with the results of Indegree on that component
- Code/: Contains the code of the project
- Heatmap:   a comprehensive visual overview of  the dataset. 
