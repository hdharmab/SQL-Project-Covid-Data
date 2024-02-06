/*
SQL Project Using Covid Data from https://ourworldindata.org/covid-deaths 
and https://www.youtube.com/@AlexTheAnalyst
The covid data is from Jan 1st 2020 - April 30th 2021
The data comprises of 2 tables
	1. CovidDeaths : contains information about deaths caused by Covid
	2. CovidVaccinations : contains information about vaccinations
*/

-- examining the CovidDeaths data
Select *
From SQLPortfolioProject..CovidDeaths

-- examining the CovidVaccinations data
Select *
From SQLPortfolioProject..CovidVaccinations

-- ratio of total_deaths to total_cases in all countries
Select continent,location, date, total_cases, total_deaths, Round((cast(total_deaths as int)/total_cases)*100,4) as RatioDeathstoCases
From SQLPortfolioProject..CovidDeaths
where continent is not null

-- ratio of total_deaths to total_cases in a specific country
Select continent, location, date, total_cases, total_deaths, Round((cast(total_deaths as int)/total_cases)*100,4) as RatioDeathstoCases
From SQLPortfolioProject..CovidDeaths
where location = 'New Zealand'

-- ratio of total cases to population in all countries
Select continent, location, date, total_cases, population, (total_cases/population)*100 as Percent_Infection
From SQLPortfolioProject..CovidDeaths
where continent is not null

-- ratio of total cases to population in a single country
Select continent, location, date, total_cases, population, (total_cases/population)*100 as Percent_Infection
From SQLPortfolioProject..CovidDeaths
where location = 'New Zealand'

-- Maximum infection rates of all countries 
Select location, MAX(total_cases) as HighestInfectionCount, population, (MAX(total_cases)/population)*100 as Max_PercentInfection
From SQLPortfolioProject..CovidDeaths
where continent is not null
Group By Location, Population
Order By Max_PercentInfection Desc

--Maximum Infection Rates by Continent
Select location, MAX(total_cases) as HighestInfectionCount, population, (MAX(total_cases)/population)*100 as Max_PercentInfection
From SQLPortfolioProject..CovidDeaths
Where Continent is Null and location <> 'International'
Group By Location, Population
Order By Max_PercentInfection Desc

-- Maximum death counts by country
Select location, MAX(cast(total_deaths as int)) as Max_TotalDeaths
From SQLPortfolioProject..CovidDeaths
where continent is not null
Group By Location
Order By Max_TotalDeaths Desc


-- Examine the growth of Covid in Countries using new_cases and new_deaths

-- the sum of new_cases, and new_deaths globally, each day 
-- excludes data where location is the continent or region , and international data
select date, sum(new_cases) Sum_NewCases, sum(convert(int, new_deaths)) Sum_NewDeaths
from SQLPortfolioProject..CovidDeaths
where continent is not null
Group By date
Order By date

-- change in the ratio of the daily total new_deaths to total new_cases 
select date, sum(new_cases) Sum_NewCases, sum(convert(int, new_deaths)) Sum_NewDeaths, round((sum(convert(int, new_deaths))/sum(new_cases))*100,4) as Ratio_NewDeathstoNewCases
from SQLPortfolioProject..CovidDeaths
where continent is not null
Group By date
Order By date

-- find the world death percentage using location data
with CTE_maxCases_Deaths as(
	select 
		location,
		max(cast(total_deaths as int)) as Highest_Totaldeaths,
		max(total_cases) as Highest_Totalcases
		from SQLPortfolioProject..CovidDeaths
		where continent is not null
		Group by Location
)		
Select 
	sum(Highest_Totaldeaths) TotalDeathCount,
	sum(Highest_Totalcases) TotalCases, 
	(sum(Highest_Totaldeaths)/sum(Highest_Totalcases))*100
From CTE_maxCases_Deaths

-- compare to data where location is world
select 
	Max(convert(int,total_deaths)) as TotalDeathCount,
	Max(total_cases) as TotalCases,
	Max(convert(int,total_deaths))/Max(total_cases) *100
from SQLPortfolioProject..CovidDeaths
Where Location = 'World'


-- vaccination data

-- number of new vacinations per day in each country
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3


-- this query provides the sum total of new vaccinations for each country
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location) as total_NewVaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3

-- provides the rolling sum of new vaccinations administered in each country
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as RollingSum_NewVaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3

-- rolling sum of new vaccinations for New Zealand 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as RollingSum_NewVaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.location = 'New Zealand' 
order by 2,3

-- percentage of population vaccinated each day
with CTE_VaccinationsPerDay as(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as RollingSum_NewVaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null 
)
Select continent,location, date, population,RollingSum_NewVaccinations, (RollingSum_NewVaccinations/population)*100 as PercentVaccinatedDaily
From CTE_VaccinationsPerDay
where location = 'New Zealand'
Order By 2,3


-- Use a Temp Table instead of CTE
DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated(
Continent nvarchar(255), 
Location nvarchar(255),
Date datetime,
Population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)
Insert into #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as RollingSum_NewVaccinations
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null  


-- percentages from all locations
Select *, (RollingPeopleVaccinated/Population)*100 AS Percent_Vaccinated
From #PercentPopulationVaccinated
Order By 2,3

-- percentages from United States
Select *, (RollingPeopleVaccinated/Population)*100 AS Percent_Vaccinated
From #PercentPopulationVaccinated
Where location = 'United States'
Order By 2,3 DESC

--CREATE VIEW PeopleVaccinatedUnitedStates as 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, sum(convert(int, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
FROM SQLPortfolioProject..CovidDeaths dea
Join SQLPortfolioProject..CovidVaccinations vac
	on dea.location = vac.location 
	and dea.date = vac.date
where dea.continent is not null and dea.location = 'United States'

SELECT *
FROM PeopleVaccinatedUnitedStates