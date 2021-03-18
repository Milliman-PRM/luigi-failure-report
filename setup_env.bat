@echo off
rem ### CODE OWNERS: Ben Copeland
rem
rem ### OBJECTIVE:
rem   Setup the environment.
rem
rem ### DEVELOPER NOTES:
rem   

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Setting up environment for usage/testing
echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Running from %~f0

rem ### LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE


echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling most recent pipeline_components_env.bat
call S:\PRM\Pipeline_Components_Env\pipeline_components_env.bat

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Explicitly adding this repository to PythonPath
set PYTHONPATH=%~dp0/python;%PYTHONPATH%

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Finished setting up environment for usage/testing
