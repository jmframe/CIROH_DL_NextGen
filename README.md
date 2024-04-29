# Deep learning for NextGen
This is a landing page for the CIROH project to integrate and test deep learning (DL) and physics-informed deep learning (PIML) streamflow models and data assimilation into NOAA’s Next Generation Water Modeling Engine and Framework Prototype.

## Links
Below are some usefull links that will be heavily used in the project.
* Next Generation Water Resources Modeling Framework Prototype: https://github.com/NOAA-OWP/ngen
* Deep learning
  * Training: https://github.com/neuralhydrology/neuralhydrology
  * BMI models for NextGen:
    * https://github.com/NOAA-OWP/lstm
    * https://github.com/hellkite500/bmi_pytorch
* Forcing:
  * Code:
    * https://github.com/CIROH-UA/ngen-datastream/tree/main/forcingprocessor
    * https://github.com/NOAA-OWP/ngen-forcing
    * https://github.com/jmframe/aorc_historic
  * Data:
    * AORC at 4KM: https://hydrology.nws.noaa.gov/aorc-historic/
* Hydrofabric:
  * What is it? https://mikejohnson51.github.io/hyAggregate/
  * Code: https://github.com/NOAA-OWP/hydrofabric
  * Data: https://www.lynker-spatial.com/#hydrofabric

## Project management
This board includes tasks to do, in progress and completed for the project: https://github.com/users/jmframe/projects/1


## References - NeuralHydrology development and experiments
* Araki, R., Bindas, T., Bhuiyan, S. A., Rapp, J., McMillan, H. K., Ogden, F. L., & Frame, J. M. (2023). Enhancing the Conceptual Functional Equivalent (CFE) rainfall-runoff model via a differentiable modeling approach. Poster session presented at the AGU Fall Meeting 2023. Retrieved from https://agu.confex.com/agu/fm23/meetingapp.cgi/Paper/1275704  
* Abramowitz et al., 2023, On the predictability of turbulent fluxes from land: PLUMBER2 MIP experimental description and preliminary results. In review for Biogeosciences.  https://egusphere.copernicus.org/preprints/2024/egusphere-2023-3084/egusphere-2023-3084.pdf  
* Bhuiya et al. 2023. “Representing soil physical processes in Conceptual Framework Equivalent (CFE) through the implementation of Ordinary Differential Equation (ODE)”. AGU Fall Meeting  
* Brenner et al., 2021, “Predicting evapotranspiration using machine and deep learning methods”. Österreichische Wasser- und Abfallwirtschaft volume 73, pages 295–307 (2021). https://link.springer.com/article/10.1007/s00506-021-00768-y  
* Feng, D., Liu, J., Lawson, K., & Shen, C. (2022). Differentiable, learnable, regionalized process-based models with physical outputs can approach state-of-the-art hydrologic prediction accuracy. Water Resources Research.  
* Foroumandi et al., 2023. “Development of an Ensemble Hydrologic Data Assimilation within NextGen Framework”. AGU Fall Meeting
* Frame J.M., Deep Learning for Operational Streamflow Forecasts: A Long Short-Term Memory Network Rainfall-Runoff Module for The National Water Model. The University of Alabama. https://ir.ua.edu/bitstream/handle/123456789/9436/u0015_0000001_0004409.pdf?sequence=1&isAllowed=y  
* Frame et al., 2023, “On strictly enforced mass conservation constraints for modeling the rainfall runoff process”. Hydrological Processes. https://onlinelibrary.wiley.com/doi/10.1002/hyp.14847  
* Frame et al., 2022, “Deep learning rainfall-runoff predictions of extreme events”. Hydrology and Earth System Sciences. https://hess.copernicus.org/articles/26/3377/2022/hess-26-3377-2022.pdf  
* Frame et al., 2021, “Post-processing the National Water Model with Long Short-Term Memory Networks for Streamflow Predictions and Model Diagnostics”. Journal of American Water Resources Association. https://onlinelibrary.wiley.com/doi/10.1111/1752-1688.12964?af=R  
* Gauch, M., Kratzert, F., Klotz, D., Nearing, G., Lin, J, Hochreiter, S.; Rainfall–Runoff Prediction at Multiple Timescales with a Single Long Short-Term Memory Network (2021): Hydrology and Earth System Sciences.  
* Gholizadeh et al., 2023, “Long short-term memory models to quantify long-term evolution of streamflow discharge and groundwater depth in Alabama”. Science of the Total Environment.  
* Kratzert, F., Klotz, D., Shalev, G., Klambauer, G., Hochreiter, S., and Nearing, G. (2019a): Towards learning universal, regional, and local hydrological behaviors via machine learning applied to large-sample datasets. Hydrology and Earth System Sciences.  
* Kratzert, F., Klotz, D., Herrnegger, M., Sampson, A. K., Hochreiter, S., and Nearing, G. S. (2019b): Towards Improved Predictions in Ungauged Basins: Exploiting the Power of Machine Learning. Water Resources Research.  
* Kratzert, F., Gauch, M., Nearing, G., and Klotz, D. (2022). NeuralHydrology — A Python library for Deep Learning research in hydrology. Journal of Open Source Software.  
* Liu, Q., Bolotin, L., Haces-Garcia, F., Liao, M., Ogden, F. L., & Frame, J. M. (2022). Automated Decision Support for Model Selection in the NextGen National Water Model. Poster session presented at the AGU Fall Meeting 2022, December 15, McCormick Place, Chicago, IL. Retrieved from https://agu.confex.com/agu/fm22/meetingapp.cgi/Paper/1189555  
* Nearing et al., 2020, “What Role Does Hydrological Science Play in the Age of Machine Learning?”. Water Resources Research. doi.org/10.1029/2020WR028091  
* Nearing et al., 2022, “Data assimilation and autoregression for using near-real-time streamflow observations in long short-term memory networks''. Hydrology and Earth System Sciences.  
* Nearing, G., Cohen, D., Dube, V., Gauch, M., Gilon, O., Harrigan, S., . . . Matias, Y. (2024, 3). Global prediction of extreme floods in ungauged watersheds. Nature, 627 , 559-563. Retrieved from https://www.nature.com/articles/s41586-024-07145-1 doi: 10.1038/s41586-024-07145-1  
* Ogden, F. L., Avant, B., Blodgett, D., Clark, E., Coon, E., Cosgrove, B., Cui, S., Kindl da Cunha, L., Farthing, M., Flowers, T., Frame, J. M., Frazier, N. J., Graziano, T., Guten- son, J., Johnson, D. W., Loney, D., Mattern, D., McDaniel, R., Moulton, J., Peckham, S. D., Jennings, K., Savant, G., Tubbs, C., Williamson, M., Garrett, J., Wood, A., and Johnson, J. M. (2021): The next generation water resources modeling framework: Open source, standards based, community accessible, model interoperability for large scale water prediction. American Geophysical Union Fall Meeting 2021.  
* Peckham, S. D., Hutton, E. W., and Norris, B. (2013): A component-based approach to integrated modeling in the geosciences: The design of CSDMS. Computers and Geosciences.  
* Rapp et al. 2023. “Value of Hydrofabric Artifact Static Parameters for Deep Learning Next Generation National Water Model (NextGen) Development”. AGU Fall Meeting  
* Timilsina et al. 2023. “Data Assimilation in the NextGen Framework for Improved Streamflow Predictions”. AGU Fall Meeting.  