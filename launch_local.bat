@echo off
rem ---------------------------------------------------------------------------
rem Anaconda location path
set ANACONDA_HOME=C:\ProgramData\Anaconda3

rem development configuration
rem set ANACONDA_HOME=C:\Users\SYLVAZE\AppData\Local\Continuum\anaconda3
set PYTHONPATH=F:\SISTDAD\MEMPGRP\BMORIER\python\lib
rem ---------------------------------------------------------------------------

echo Starting conda env [envML]
call %ANACONDA_HOME%\Scripts\activate.bat envML

call python Q:\source\python\projects\shooter_server\app.py

pause