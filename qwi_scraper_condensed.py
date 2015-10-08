# qwi_scraper_condensed.py
# execfile('/Users/aclemens/Desktop/Projects/QWI Data Portal/QWI_backend/qwi_scraper_condensed.py')
from __future__ import division
import urllib2
import re
import string
import gzip
import csv
from StringIO import StringIO
import pandas as pd
import os

# Full path to example file: 
# lehd.ces.census.gov/pub/ak/latest_release/DVD-sa_f/qwi_ak_sa_f_gc_ns_op_u.csv.gz
# With variable replacements:
# lehd.ces.census.gov/pub/STATE/latest_release/BASEFOLDER/qwi_STATE_DEMOG_FIRMABREV_INDUSTRY_OWNERSHIP_u.csv.gz

# scrape() parameters
base_folder='DVD-sa_f'
geog='gs'
firm_abrev='f'
demog='sa'
industry='n3'
ownership='oslp'

occ_codes=[['10', 'Chief executives and legislators/public administration', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['20', 'General and Operations Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['30', 'Managers in Marketing, Advertising, and Public Relations', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['100', 'Administrative Services Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['110', 'Computer and Information Systems Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['120', 'Financial Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['130', 'Human Resources Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['140', 'Industrial Production Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['150', 'Purchasing Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['160', 'Transportation, Storage, and Distribution Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['205', 'Farmers, Ranchers, and Other Agricultural Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['220', 'Constructions Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['230', 'Education Administrators', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['300', 'Architectural and Engineering Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['310', 'Food Service and Lodging Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['320', 'Funeral Directors', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['330', 'Gaming Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['350', 'Medical and Health Services Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['360', 'Natural Science Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['410', 'Property, Real Estate, and Community Association Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['420', 'Social and Community Service Managers', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['430', 'Managers, nec (including Postmasters)', 'MANAGEMENT, BUSINESS, SCIENCE, AND ARTS'], ['500', 'Agents and Business Managers of Artists, Performers, and Athletes', 'BUSINESS OPERATIONS SPECIALISTS'], ['510', 'Buyers and Purchasing Agents, Farm Products', 'BUSINESS OPERATIONS SPECIALISTS'], ['520', 'Wholesale and Retail Buyers, Except Farm Products', 'BUSINESS OPERATIONS SPECIALISTS'], ['530', 'Purchasing Agents, Except Wholesale, Retail, and Farm Products', 'BUSINESS OPERATIONS SPECIALISTS'], ['540', 'Claims Adjusters, Appraisers, Examiners, and Investigators', 'BUSINESS OPERATIONS SPECIALISTS'], ['560', 'Compliance Officers, Except Agriculture', 'BUSINESS OPERATIONS SPECIALISTS'], ['600', 'Cost Estimators', 'BUSINESS OPERATIONS SPECIALISTS'], ['620', 'Human Resources, Training, and Labor Relations Specialists', 'BUSINESS OPERATIONS SPECIALISTS'], ['700', 'Logisticians', 'BUSINESS OPERATIONS SPECIALISTS'], ['710', 'Management Analysts', 'BUSINESS OPERATIONS SPECIALISTS'], ['720', 'Meeting and Convention Planners', 'BUSINESS OPERATIONS SPECIALISTS'], ['730', 'Other Business Operations and Management Specialists', 'BUSINESS OPERATIONS SPECIALISTS'], ['800', 'Accountants and Auditors', 'FINANCIAL SPECIALISTS'], ['810', 'Appraisers and Assessors of Real Estate', 'FINANCIAL SPECIALISTS'], ['820', 'Budget Analysts', 'FINANCIAL SPECIALISTS'], ['830', 'Credit Analysts', 'FINANCIAL SPECIALISTS'], ['840', 'Financial Analysts', 'FINANCIAL SPECIALISTS'], ['850', 'Personal Financial Advisors', 'FINANCIAL SPECIALISTS'], ['860', 'Insurance Underwriters', 'FINANCIAL SPECIALISTS'], ['900', 'Financial Examiners', 'FINANCIAL SPECIALISTS'], ['910', 'Credit Counselors and Loan Officers', 'FINANCIAL SPECIALISTS'], ['930', 'Tax Examiners and Collectors, and Revenue Agents', 'FINANCIAL SPECIALISTS'], ['940', 'Tax Preparers', 'FINANCIAL SPECIALISTS'], ['950', 'Financial Specialists, nec', 'FINANCIAL SPECIALISTS'], ['1000', 'Computer Scientists and Systems Analysts/Network systems Analysts/Web Developers', 'COMPUTER AND MATHEMATICAL'], ['1010', 'Computer Programmers', 'COMPUTER AND MATHEMATICAL'], ['1020', 'Software Developers, Applications and Systems Software', 'COMPUTER AND MATHEMATICAL'], ['1050', 'Computer Support Specialists', 'COMPUTER AND MATHEMATICAL'], ['1060', 'Database Administrators', 'COMPUTER AND MATHEMATICAL'], ['1100', 'Network and Computer Systems Administrators', 'COMPUTER AND MATHEMATICAL'], ['1200', 'Actuaries', 'COMPUTER AND MATHEMATICAL'], ['1220', 'Operations Research Analysts', 'COMPUTER AND MATHEMATICAL'], ['1230', 'Statisticians', 'COMPUTER AND MATHEMATICAL'], ['1240', 'Mathematical science occupations, nec', 'COMPUTER AND MATHEMATICAL'], ['1300', 'Architects, Except Naval', 'ARCHITECTURE AND ENGINEERING'], ['1310', 'Surveyors, Cartographers, and Photogrammetrists', 'ARCHITECTURE AND ENGINEERING'], ['1320', 'Aerospace Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1350', 'Chemical Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1360', 'Civil Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1400', 'Computer Hardware Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1410', 'Electrical and Electronics Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1420', 'Environmental Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1430', 'Industrial Engineers, including Health and Safety', 'ARCHITECTURE AND ENGINEERING'], ['1440', 'Marine Engineers and Naval Architects', 'ARCHITECTURE AND ENGINEERING'], ['1450', 'Materials Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1460', 'Mechanical Engineers', 'ARCHITECTURE AND ENGINEERING'], ['1520', 'Petroleum, mining and geological engineers, including mining safety engineers', 'ARCHITECTURE AND ENGINEERING'], ['1530', 'Engineers, nec', 'ARCHITECTURE AND ENGINEERING'], ['1540', 'Drafters', 'ARCHITECTURE AND ENGINEERING'], ['1550', 'Engineering Technicians, Except Drafters', 'TECHNICIANS'], ['1560', 'Surveying and Mapping Technicians', 'TECHNICIANS'], ['1600', 'Agricultural and Food Scientists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1610', 'Biological Scientists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1640', 'Conservation Scientists and Foresters', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1650', 'Medical Scientists, and Life Scientists, All Other', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1700', 'Astronomers and Physicists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1710', 'Atmospheric and Space Scientists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1720', 'Chemists and Materials Scientists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1740', 'Environmental Scientists and Geoscientists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1760', 'Physical Scientists, nec', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1800', 'Economists and market researchers', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1810', '', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1820', 'Psychologists', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1830', 'Urban and Regional Planners', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1840', 'Social Scientists, nec', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1900', 'Agricultural and Food Science Technicians', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1910', 'Biological Technicians', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1920', 'Chemical Technicians', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1930', 'Geological and Petroleum Technicians, and Nuclear Technicians', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1960', 'Life, Physical, and Social Science Technicians, nec', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['1980', 'Professional, Research, or Technical Workers, nec', 'LIFE, PHYSICAL, AND SOCIAL SCIENCE'], ['2000', 'Counselors', 'COMMUNITY AND SOCIAL SERVICES'], ['2010', 'Social Workers', 'COMMUNITY AND SOCIAL SERVICES'], ['2020', 'Community and Social Service Specialists, nec', 'COMMUNITY AND SOCIAL SERVICES'], ['2040', 'Clergy', 'COMMUNITY AND SOCIAL SERVICES'], ['2050', 'Directors, Religious Activities and Education', 'COMMUNITY AND SOCIAL SERVICES'], ['2060', 'Religious Workers, nec', 'COMMUNITY AND SOCIAL SERVICES'], ['2100', 'Lawyers, and judges, magistrates, and other judicial workers', 'LEGAL'], ['2140', 'Paralegals and Legal Assistants', 'LEGAL'], ['2150', 'Legal Support Workers, nec', 'LEGAL'], ['2200', 'Postsecondary Teachers', 'EDUCATION, TRAINING, AND LIBRARY'], ['2300', 'Preschool and Kindergarten Teachers', 'EDUCATION, TRAINING, AND LIBRARY'], ['2310', 'Elementary and Middle School Teachers', 'EDUCATION, TRAINING, AND LIBRARY'], ['2320', 'Secondary School Teachers', 'EDUCATION, TRAINING, AND LIBRARY'], ['2330', 'Special Education Teachers', 'EDUCATION, TRAINING, AND LIBRARY'], ['2340', 'Other Teachers and Instructors', 'EDUCATION, TRAINING, AND LIBRARY'], ['2400', 'Archivists, Curators, and Museum Technicians', 'EDUCATION, TRAINING, AND LIBRARY'], ['2430', 'Librarians', 'EDUCATION, TRAINING, AND LIBRARY'], ['2440', 'Library Technicians', 'EDUCATION, TRAINING, AND LIBRARY'], ['2540', 'Teacher Assistants', 'EDUCATION, TRAINING, AND LIBRARY'], ['2550', 'Education, Training, and Library Workers, nec', 'EDUCATION, TRAINING, AND LIBRARY'], ['2600', 'Artists and Related Workers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2630', 'Designers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2700', 'Actors, Producers, and Directors', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2720', 'Athletes, Coaches, Umpires, and Related Workers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2740', 'Dancers and Choreographers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2750', 'Musicians, Singers, and Related Workers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2760', 'Entertainers and Performers, Sports and Related Workers, All Other', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2800', 'Announcers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2810', 'Editors, News Analysts, Reporters, and Correspondents', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2825', 'Public Relations Specialists', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2840', 'Technical Writers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2850', 'Writers and Authors', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2860', 'Media and Communication Workers, nec', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2900', 'Broadcast and Sound Engineering Technicians and Radio Operators, and media and communication equipment workers, all other', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2910', 'Photographers', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['2920', 'Television, Video, and Motion Picture Camera Operators and Editors', 'ARTS, DESIGN, ENTERTAINMENT, SPORTS, AND MEDIA'], ['3000', 'Chiropractors', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3010', 'Dentists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3030', 'Dieticians and Nutritionists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3040', 'Optometrists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3050', 'Pharmacists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3060', 'Physicians and Surgeons', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3110', 'Physician Assistants', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3120', 'Podiatrists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3130', 'Registered Nurses', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3140', 'Audiologists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3150', 'Occupational Therapists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3160', 'Physical Therapists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3200', 'Radiation Therapists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3210', 'Recreational Therapists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3220', 'Respiratory Therapists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3230', 'Speech Language Pathologists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3240', 'Therapists, nec', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3250', 'Veterinarians', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3260', 'Health Diagnosing and Treating Practitioners, nec', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3300', 'Clinical Laboratory Technologists and Technicians', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3310', 'Dental Hygienists', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3320', 'Diagnostic Related Technologists and Technicians', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3400', 'Emergency Medical Technicians and Paramedics', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3410', 'Health Diagnosing and Treating Practitioner Support Technicians', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3500', 'Licensed Practical and Licensed Vocational Nurses', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3510', 'Medical Records and Health Information Technicians', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3520', 'Opticians, Dispensing', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3530', 'Health Technologists and Technicians, nec', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3540', 'Healthcare Practitioners and Technical Occupations, nec', 'HEALTHCARE PRACTITIONERS AND TECHNICAL'], ['3600', 'Nursing, Psychiatric, and Home Health Aides', 'HEALTHCARE SUPPORT'], ['3610', 'Occupational Therapy Assistants and Aides', 'HEALTHCARE SUPPORT'], ['3620', 'Physical Therapist Assistants and Aides', 'HEALTHCARE SUPPORT'], ['3630', 'Massage Therapists', 'HEALTHCARE SUPPORT'], ['3640', 'Dental Assistants', 'HEALTHCARE SUPPORT'], ['3650', 'Medical Assistants and Other Healthcare Support Occupations, nec', 'HEALTHCARE SUPPORT'], ['3700', 'First-Line Supervisors of Correctional Officers', 'PROTECTIVE SERVICE'], ['3710', 'First-Line Supervisors of Police and Detectives', 'PROTECTIVE SERVICE'], ['3720', 'First-Line Supervisors of Fire Fighting and Prevention Workers', 'PROTECTIVE SERVICE'], ['3730', 'Supervisors, Protective Service Workers, All Other', 'PROTECTIVE SERVICE'], ['3740', 'Firefighters', 'PROTECTIVE SERVICE'], ['3750', 'Fire Inspectors', 'PROTECTIVE SERVICE'], ['3800', 'Sheriffs, Bailiffs, Correctional Officers, and Jailers', 'PROTECTIVE SERVICE'], ['3820', 'Police Officers and Detectives', 'PROTECTIVE SERVICE'], ['3900', 'Animal Control', 'PROTECTIVE SERVICE'], ['3910', 'Private Detectives and Investigators', 'PROTECTIVE SERVICE'], ['3930', 'Security Guards and Gaming Surveillance Officers', 'PROTECTIVE SERVICE'], ['3940', 'Crossing Guards', 'PROTECTIVE SERVICE'], ['3950', 'Law enforcement workers, nec', 'PROTECTIVE SERVICE'], ['4000', 'Chefs and Cooks', 'FOOD PREPARATION AND SERVING'], ['4010', 'First-Line Supervisors of Food Preparation and Serving Workers', 'FOOD PREPARATION AND SERVING'], ['4030', 'Food Preparation Workers', 'FOOD PREPARATION AND SERVING'], ['4040', 'Bartenders', 'FOOD PREPARATION AND SERVING'], ['4050', 'Combined Food Preparation and Serving Workers, Including Fast Food', 'FOOD PREPARATION AND SERVING'], ['4060', 'Counter Attendant, Cafeteria, Food Concession, and Coffee Shop', 'FOOD PREPARATION AND SERVING'], ['4110', 'Waiters and Waitresses', 'FOOD PREPARATION AND SERVING'], ['4120', 'Food Servers, Nonrestaurant', 'FOOD PREPARATION AND SERVING'], ['4130', 'Food preparation and serving related workers, nec', 'FOOD PREPARATION AND SERVING'], ['4140', 'Dishwashers', 'FOOD PREPARATION AND SERVING'], ['4150', 'Host and Hostesses, Restaurant, Lounge, and Coffee Shop', 'FOOD PREPARATION AND SERVING'], ['4200', 'First-Line Supervisors of Housekeeping and Janitorial Workers', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4210', 'First-Line Supervisors of Landscaping, Lawn Service, and Groundskeeping Workers', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4220', 'Janitors and Building Cleaners', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4230', 'Maids and Housekeeping Cleaners', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4240', 'Pest Control Workers', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4250', 'Grounds Maintenance Workers', 'BUILDING AND GROUNDS CLEANING AND MAINTENANCE'], ['4300', 'First-Line Supervisors of Gaming Workers', 'PERSONAL CARE AND SERVICE'], ['4320', 'First-Line Supervisors of Personal Service Workers', 'PERSONAL CARE AND SERVICE'], ['4340', 'Animal Trainers', 'PERSONAL CARE AND SERVICE'], ['4350', 'Nonfarm Animal Caretakers', 'PERSONAL CARE AND SERVICE'], ['4400', 'Gaming Services Workers', 'PERSONAL CARE AND SERVICE'], ['4420', 'Ushers, Lobby Attendants, and Ticket Takers', 'PERSONAL CARE AND SERVICE'], ['4430', 'Entertainment Attendants and Related Workers, nec', 'PERSONAL CARE AND SERVICE'], ['4460', 'Funeral Service Workers and Embalmers', 'PERSONAL CARE AND SERVICE'], ['4500', 'Barbers', 'PERSONAL CARE AND SERVICE'], ['4510', 'Hairdressers, Hairstylists, and Cosmetologists', 'PERSONAL CARE AND SERVICE'], ['4520', 'Personal Appearance Workers, nec', 'PERSONAL CARE AND SERVICE'], ['4530', 'Baggage Porters, Bellhops, and Concierges', 'PERSONAL CARE AND SERVICE'], ['4540', 'Tour and Travel Guides', 'PERSONAL CARE AND SERVICE'], ['4600', 'Childcare Workers', 'PERSONAL CARE AND SERVICE'], ['4610', 'Personal Care Aides', 'PERSONAL CARE AND SERVICE'], ['4620', 'Recreation and Fitness Workers', 'PERSONAL CARE AND SERVICE'], ['4640', 'Residential Advisors', 'PERSONAL CARE AND SERVICE'], ['4650', 'Personal Care and Service Workers, All Other', 'PERSONAL CARE AND SERVICE'], ['4700', 'First-Line Supervisors of Sales Workers', 'SALES AND RELATED'], ['4720', 'Cashiers', 'SALES AND RELATED'], ['4740', 'Counter and Rental Clerks', 'SALES AND RELATED'], ['4750', 'Parts Salespersons', 'SALES AND RELATED'], ['4760', 'Retail Salespersons', 'SALES AND RELATED'], ['4800', 'Advertising Sales Agents', 'SALES AND RELATED'], ['4810', 'Insurance Sales Agents', 'SALES AND RELATED'], ['4820', 'Securities, Commodities, and Financial Services Sales Agents', 'SALES AND RELATED'], ['4830', 'Travel Agents', 'SALES AND RELATED'], ['4840', 'Sales Representatives, Services, All Other', 'SALES AND RELATED'], ['4850', 'Sales Representatives, Wholesale and Manufacturing', 'SALES AND RELATED'], ['4900', 'Models, Demonstrators, and Product Promoters', 'SALES AND RELATED'], ['4920', 'Real Estate Brokers and Sales Agents', 'SALES AND RELATED'], ['4930', 'Sales Engineers', 'SALES AND RELATED'], ['4940', 'Telemarketers', 'SALES AND RELATED'], ['4950', 'Door-to-Door Sales Workers, News and Street Vendors, and Related Workers', 'SALES AND RELATED'], ['4965', 'Sales and Related Workers, All Other', 'SALES AND RELATED'], ['5000', 'First-Line Supervisors of Office and Administrative Support Workers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5010', 'Switchboard Operators, Including Answering Service', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5020', 'Telephone Operators', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5030', 'Communications Equipment Operators, All Other', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5100', 'Bill and Account Collectors', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5110', 'Billing and Posting Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5120', 'Bookkeeping, Accounting, and Auditing Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5130', 'Gaming Cage Workers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5140', 'Payroll and Timekeeping Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5150', 'Procurement Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5160', 'Bank Tellers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5165', 'Financial Clerks, nec', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5200', 'Brokerage Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5220', 'Court, Municipal, and License Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5230', 'Credit Authorizers, Checkers, and Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5240', 'Customer Service Representatives', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5250', 'Eligibility Interviewers, Government Programs', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5260', 'File Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5300', 'Hotel, Motel, and Resort Desk Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5310', 'Interviewers, Except Eligibility and Loan', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5320', 'Library Assistants, Clerical', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5330', 'Loan Interviewers and Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5340', 'New Account Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5350', 'Correspondent clerks and order clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5360', 'Human Resources Assistants, Except Payroll and Timekeeping', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5400', 'Receptionists and Information Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5410', 'Reservation and Transportation Ticket Agents and Travel Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5420', 'Information and Record Clerks, All Other', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5500', 'Cargo and Freight Agents', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5510', 'Couriers and Messengers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5520', 'Dispatchers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5530', 'Meter Readers, Utilities', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5540', 'Postal Service Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5550', 'Postal Service Mail Carriers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5560', 'Postal Service Mail Sorters, Processors, and Processing Machine Operators', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5600', 'Production, Planning, and Expediting Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5610', 'Shipping, Receiving, and Traffic Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5620', 'Stock Clerks and Order Fillers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5630', 'Weighers, Measurers, Checkers, and Samplers, Recordkeeping', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5700', 'Secretaries and Administrative Assistants', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5800', 'Computer Operators', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5810', 'Data Entry Keyers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5820', 'Word Processors and Typists', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5840', 'Insurance Claims and Policy Processing Clerks', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5850', 'Mail Clerks and Mail Machine Operators, Except Postal Service', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5860', 'Office Clerks, General', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5900', 'Office Machine Operators, Except Computer', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5910', 'Proofreaders and Copy Markers', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5920', 'Statistical Assistants', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['5940', 'Office and administrative support workers, nec', 'OFFICE AND ADMINISTRATIVE SUPPORT'], ['6005', 'First-Line Supervisors of Farming, Fishing, and Forestry Workers', 'FARMING, FISHING, AND FORESTRY'], ['6010', 'Agricultural Inspectors', 'FARMING, FISHING, AND FORESTRY'], ['6040', 'Graders and Sorters, Agricultural Products', 'FARMING, FISHING, AND FORESTRY'], ['6050', 'Agricultural workers, nec', 'FARMING, FISHING, AND FORESTRY'], ['6100', 'Fishing and hunting workers', 'FARMING, FISHING, AND FORESTRY'], ['6120', 'Forest and Conservation Workers', 'FARMING, FISHING, AND FORESTRY'], ['6130', 'Logging Workers', 'FARMING, FISHING, AND FORESTRY'], ['6200', 'First-Line Supervisors of Construction Trades and Extraction Workers', 'CONSTRUCTION'], ['6210', 'Boilermakers', 'CONSTRUCTION'], ['6220', 'Brickmasons, Blockmasons, and Stonemasons', 'CONSTRUCTION'], ['6230', 'Carpenters', 'CONSTRUCTION'], ['6240', 'Carpet, Floor, and Tile Installers and Finishers', 'CONSTRUCTION'], ['6250', 'Cement Masons, Concrete Finishers, and Terrazzo Workers', 'CONSTRUCTION'], ['6260', 'Construction Laborers', 'CONSTRUCTION'], ['6300', 'Paving, Surfacing, and Tamping Equipment Operators', 'CONSTRUCTION'], ['6320', 'Construction equipment operators except paving, surfacing, and tamping equipment operators', 'CONSTRUCTION'], ['6330', 'Drywall Installers, Ceiling Tile Installers, and Tapers', 'CONSTRUCTION'], ['6355', 'Electricians', 'CONSTRUCTION'], ['6360', 'Glaziers', 'CONSTRUCTION'], ['6400', 'Insulation Workers', 'CONSTRUCTION'], ['6420', 'Painters, Construction and Maintenance', 'CONSTRUCTION'], ['6430', 'Paperhangers', 'CONSTRUCTION'], ['6440', 'Pipelayers, Plumbers, Pipefitters, and Steamfitters', 'CONSTRUCTION'], ['6460', 'Plasterers and Stucco Masons', 'CONSTRUCTION'], ['6500', 'Reinforcing Iron and Rebar Workers', 'CONSTRUCTION'], ['6515', 'Roofers', 'CONSTRUCTION'], ['6520', 'Sheet Metal Workers, metal-working', 'CONSTRUCTION'], ['6530', 'Structural Iron and Steel Workers', 'CONSTRUCTION'], ['6600', 'Helpers, Construction Trades', 'CONSTRUCTION'], ['6660', 'Construction and Building Inspectors', 'CONSTRUCTION'], ['6700', 'Elevator Installers and Repairers', 'CONSTRUCTION'], ['6710', 'Fence Erectors', 'CONSTRUCTION'], ['6720', 'Hazardous Materials Removal Workers', 'CONSTRUCTION'], ['6730', 'Highway Maintenance Workers', 'CONSTRUCTION'], ['6740', 'Rail-Track Laying and Maintenance Equipment Operators', 'CONSTRUCTION'], ['6765', 'Construction workers, nec', 'CONSTRUCTION'], ['6800', 'Derrick, rotary drill, and service unit operators, and roustabouts, oil, gas, and mining', 'EXTRACTION'], ['6820', 'Earth Drillers, Except Oil and Gas', 'EXTRACTION'], ['6830', 'Explosives Workers, Ordnance Handling Experts, and Blasters', 'EXTRACTION'], ['6840', 'Mining Machine Operators', 'EXTRACTION'], ['6940', 'Extraction workers, nec', 'EXTRACTION'], ['7000', 'First-Line Supervisors of Mechanics, Installers, and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7010', 'Computer, Automated Teller, and Office Machine Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7020', 'Radio and Telecommunications Equipment Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7030', 'Avionics Technicians', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7040', 'Electric Motor, Power Tool, and Related Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7100', 'Electrical and electronics repairers, transportation equipment, and industrial and utility', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7110', 'Electronic Equipment Installers and Repairers, Motor Vehicles', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7120', 'Electronic Home Entertainment Equipment Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7125', 'Electronic Repairs, nec', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7130', 'Security and Fire Alarm Systems Installers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7140', 'Aircraft Mechanics and Service Technicians', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7150', 'Automotive Body and Related Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7160', 'Automotive Glass Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7200', 'Automotive Service Technicians and Mechanics', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7210', 'Bus and Truck Mechanics and Diesel Engine Specialists', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7220', 'Heavy Vehicle and Mobile Equipment Service Technicians and Mechanics', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7240', 'Small Engine Mechanics', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7260', 'Vehicle and Mobile Equipment Mechanics, Installers, and Repairers, nec', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7300', 'Control and Valve Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7315', 'Heating, Air Conditioning, and Refrigeration Mechanics and Installers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7320', 'Home Appliance Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7330', 'Industrial and Refractory Machinery Mechanics', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7340', 'Maintenance and Repair Workers, General', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7350', 'Maintenance Workers, Machinery', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7360', 'Millwrights', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7410', 'Electrical Power-Line Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7420', 'Telecommunications Line Installers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7430', 'Precision Instrument and Equipment Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7510', 'Coin, Vending, and Amusement Machine Servicers and Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7540', 'Locksmiths and Safe Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7550', 'Manufactured Building and Mobile Home Installers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7560', 'Riggers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7610', 'Helpers--Installation, Maintenance, and Repair Workers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7630', 'Other Installation, Maintenance, and Repair Workers Including Wind Turbine Service Technicians, and Commercial Divers, and Signal and Track Switch Repairers', 'INSTALLATION, MAINTENANCE, AND REPAIR'], ['7700', 'First-Line Supervisors of Production and Operating Workers', 'PRODUCTION'], ['7710', 'Aircraft Structure, Surfaces, Rigging, and Systems Assemblers', 'PRODUCTION'], ['7720', 'Electrical, Electronics, and Electromechanical Assemblers', 'PRODUCTION'], ['7730', 'Engine and Other Machine Assemblers', 'PRODUCTION'], ['7740', 'Structural Metal Fabricators and Fitters', 'PRODUCTION'], ['7750', 'Assemblers and Fabricators, nec', 'PRODUCTION'], ['7800', 'Bakers', 'PRODUCTION'], ['7810', 'Butchers and Other Meat, Poultry, and Fish Processing Workers', 'PRODUCTION'], ['7830', 'Food and Tobacco Roasting, Baking, and Drying Machine Operators and Tenders', 'PRODUCTION'], ['7840', 'Food Batchmakers', 'PRODUCTION'], ['7850', 'Food Cooking Machine Operators and Tenders', 'PRODUCTION'], ['7855', 'Food Processing, nec', 'PRODUCTION'], ['7900', 'Computer Control Programmers and Operators', 'PRODUCTION'], ['7920', 'Extruding and Drawing Machine Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['7930', 'Forging Machine Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['7940', 'Rolling Machine Setters, Operators, and Tenders, metal and Plastic', 'PRODUCTION'], ['7950', 'Cutting, Punching, and Press Machine Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['7960', 'Drilling and Boring Machine Tool Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8000', 'Grinding, Lapping, Polishing, and Buffing Machine Tool Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8010', 'Lathe and Turning Machine Tool Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8030', 'Machinists', 'PRODUCTION'], ['8040', 'Metal Furnace Operators, Tenders, Pourers, and Casters', 'PRODUCTION'], ['8060', 'Model Makers and Patternmakers, Metal and Plastic', 'PRODUCTION'], ['8100', 'Molders and Molding Machine Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8130', 'Tool and Die Makers', 'PRODUCTION'], ['8140', 'Welding, Soldering, and Brazing Workers', 'PRODUCTION'], ['8150', 'Heat Treating Equipment Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8200', 'Plating and Coating Machine Setters, Operators, and Tenders, Metal and Plastic', 'PRODUCTION'], ['8210', 'Tool Grinders, Filers, and Sharpeners', 'PRODUCTION'], ['8220', 'Metal workers and plastic workers, nec', 'PRODUCTION'], ['8230', 'Bookbinders, Printing Machine Operators, and Job Printers', 'PRODUCTION'], ['8250', 'Prepress Technicians and Workers', 'PRODUCTION'], ['8300', 'Laundry and Dry-Cleaning Workers', 'PRODUCTION'], ['8310', 'Pressers, Textile, Garment, and Related Materials', 'PRODUCTION'], ['8320', 'Sewing Machine Operators', 'PRODUCTION'], ['8330', 'Shoe and Leather Workers and Repairers', 'PRODUCTION'], ['8340', 'Shoe Machine Operators and Tenders', 'PRODUCTION'], ['8350', 'Tailors, Dressmakers, and Sewers', 'PRODUCTION'], ['8400', 'Textile bleaching and dyeing, and cutting machine setters, operators, and tenders', 'PRODUCTION'], ['8410', 'Textile Knitting and Weaving Machine Setters, Operators, and Tenders', 'PRODUCTION'], ['8420', 'Textile Winding, Twisting, and Drawing Out Machine Setters, Operators, and Tenders', 'PRODUCTION'], ['8450', 'Upholsterers', 'PRODUCTION'], ['8460', 'Textile, Apparel, and Furnishings workers, nec', 'PRODUCTION'], ['8500', 'Cabinetmakers and Bench Carpenters', 'PRODUCTION'], ['8510', 'Furniture Finishers', 'PRODUCTION'], ['8530', 'Sawing Machine Setters, Operators, and Tenders, Wood', 'PRODUCTION'], ['8540', 'Woodworking Machine Setters, Operators, and Tenders, Except Sawing', 'PRODUCTION'], ['8550', 'Woodworkers including model makers and patternmakers, nec', 'PRODUCTION'], ['8600', 'Power Plant Operators, Distributors, and Dispatchers', 'PRODUCTION'], ['8610', 'Stationary Engineers and Boiler Operators', 'PRODUCTION'], ['8620', 'Water Wastewater Treatment Plant and System Operators', 'PRODUCTION'], ['8630', 'Plant and System Operators, nec', 'PRODUCTION'], ['8640', 'Chemical Processing Machine Setters, Operators, and Tenders', 'PRODUCTION'], ['8650', 'Crushing, Grinding, Polishing, Mixing, and Blending Workers', 'PRODUCTION'], ['8710', 'Cutting Workers', 'PRODUCTION'], ['8720', 'Extruding, Forming, Pressing, and Compacting Machine Setters, Operators, and Tenders', 'PRODUCTION'], ['8730', 'Furnace, Kiln, Oven, Drier, and Kettle Operators and Tenders', 'PRODUCTION'], ['8740', 'Inspectors, Testers, Sorters, Samplers, and Weighers', 'PRODUCTION'], ['8750', 'Jewelers and Precious Stone and Metal Workers', 'PRODUCTION'], ['8760', 'Medical, Dental, and Ophthalmic Laboratory Technicians', 'PRODUCTION'], ['8800', 'Packaging and Filling Machine Operators and Tenders', 'PRODUCTION'], ['8810', 'Painting Workers and Dyers', 'PRODUCTION'], ['8830', 'Photographic Process Workers and Processing Machine Operators', 'PRODUCTION'], ['8850', 'Adhesive Bonding Machine Operators and Tenders', 'PRODUCTION'], ['8860', 'Cleaning, Washing, and Metal Pickling Equipment Operators and Tenders', 'PRODUCTION'], ['8910', 'Etchers, Engravers, and Lithographers', 'PRODUCTION'], ['8920', 'Molders, Shapers, and Casters, Except Metal and Plastic', 'PRODUCTION'], ['8930', 'Paper Goods Machine Setters, Operators, and Tenders', 'PRODUCTION'], ['8940', 'Tire Builders', 'PRODUCTION'], ['8950', 'Helpers--Production Workers', 'PRODUCTION'], ['8965', 'Other production workers including semiconductor processors and cooling and freezing equipment operators', 'PRODUCTION'], ['9000', 'Supervisors of Transportation and Material Moving Workers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9030', 'Aircraft Pilots and Flight Engineers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9040', 'Air Traffic Controllers and Airfield Operations Specialists', 'TRANSPORTATION AND MATERIAL MOVING'], ['9050', 'Flight Attendants and Transportation Workers and Attendants', 'TRANSPORTATION AND MATERIAL MOVING'], ['9100', 'Bus and Ambulance Drivers and Attendants', 'TRANSPORTATION AND MATERIAL MOVING'], ['9130', 'Driver/Sales Workers and Truck Drivers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9140', 'Taxi Drivers and Chauffeurs', 'TRANSPORTATION AND MATERIAL MOVING'], ['9150', 'Motor Vehicle Operators, All Other', 'TRANSPORTATION AND MATERIAL MOVING'], ['9200', 'Locomotive Engineers and Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9230', 'Railroad Brake, Signal, and Switch Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9240', 'Railroad Conductors and Yardmasters', 'TRANSPORTATION AND MATERIAL MOVING'], ['9260', 'Subway, Streetcar, and Other Rail Transportation Workers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9300', 'Sailors and marine oilers, and ship engineers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9310', 'Ship and Boat Captains and Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9350', 'Parking Lot Attendants', 'TRANSPORTATION AND MATERIAL MOVING'], ['9360', 'Automotive and Watercraft Service Attendants', 'TRANSPORTATION AND MATERIAL MOVING'], ['9410', 'Transportation Inspectors', 'TRANSPORTATION AND MATERIAL MOVING'], ['9420', 'Transportation workers, nec', 'TRANSPORTATION AND MATERIAL MOVING'], ['9510', 'Crane and Tower Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9520', 'Dredge, Excavating, and Loading Machine Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9560', 'Conveyor operators and tenders, and hoist and winch operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9600', 'Industrial Truck and Tractor Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9610', 'Cleaners of Vehicles and Equipment', 'TRANSPORTATION AND MATERIAL MOVING'], ['9620', 'Laborers and Freight, Stock, and Material Movers, Hand', 'TRANSPORTATION AND MATERIAL MOVING'], ['9630', 'Machine Feeders and Offbearers', 'TRANSPORTATION AND MATERIAL MOVING'], ['9640', 'Packers and Packagers, Hand', 'TRANSPORTATION AND MATERIAL MOVING'], ['9650', 'Pumping Station Operators', 'TRANSPORTATION AND MATERIAL MOVING'], ['9720', 'Refuse and Recyclable Material Collectors', 'TRANSPORTATION AND MATERIAL MOVING'], ['9750', 'Material moving workers, nec', 'TRANSPORTATION AND MATERIAL MOVING'], ['9800', 'Military Officer Special and Tactical Operations Leaders', 'MILITARY SPECIFIC'], ['9810', 'First-Line Enlisted Military Supervisors', 'MILITARY SPECIFIC'], ['9820', 'Military Enlisted Tactical Operations and Air/Weapons Specialists and Crew Members', 'MILITARY SPECIFIC'], ['9830', 'Military, Rank Not Specified', 'MILITARY SPECIFIC']]
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
download_folder = '/Users/aclemens/Desktop/QWI_download/professions/'
# Drop conditions allow you to drop data that does *not* meet the conditions below. For example, the entry
# 0:'Q' drops all observations where the first column is not 'Q', which indicates quarterly data.
drop_conditions = {0:'Q',2:'S',4:'3',6:'A00',7:'0',8:'A00',9:'A0',10:'A0',11:'E0'}
time_conditions = {'start':2005,'end':2015}

# js_dict parameters
state_codes = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '2', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '8',
    'CA': '6', 'AL': '1', 'AR': '5', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '4', 'ID': '16', 'CT': '9',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46'
}
ind_codes = {
	'11':'Agriculture, Forestry, Fishing and Hunting','21':'Mining, Quarrying, and Oil and Gas Extraction','22':'Utilities','23':'Construction','31-33':'Manufacturing',
	'42':'Wholesale Trade','44-45':'Retail Trade','48-49':'Transportation and Warehousing','51':'Information','52':'Finance and Insurance',
	'53':'Real Estate and Rental and Leasing','54':'Professional, Scientific, and Technical Services','55':'Management of Companies and Enterprises',
	'56':'Administrative and Support and Waste Management and Remediation Services','61':'Educational Services','62':'Health Care and Social Assistance',
	'71':'Arts, Entertainment, and Recreation','72':'Accomodation and Food Services','81':'Other Services','92':'Public Administration'
}


def scrape():
	"""Scrape data for all states using the values set in the variables above (the scrape section). Saves each state file
	seperately and creates one combined (ALL) file. Change the string variables above to change the base URL from which data
	is drawn, change drop_conditions and time_conditions to narrow the scope of data by dropping based on time period or
	certain column values."""
	master_data=[]

	for state in states:
		try:
			state=state.lower()
			state_url='http://lehd.ces.census.gov/pub/%s/latest_release/%s/qwi_%s_%s_%s_%s_%s_%s_u.csv.gz' % (state,base_folder,state,demog,firm_abrev,geog,industry,ownership)
			print state_url
			dataset=StringIO(urllib2.urlopen(state_url).read())
			dataset=gzip.GzipFile(fileobj=dataset,mode='rb')
			dataset=dataset.read()
			dataset=dataset.split('\n')
			dataset=[row.split(',') for row in dataset]
			dataset=[row for row in dataset if len(row)==80]
			header=dataset[0]
			dataset=pd.DataFrame(dataset[1:],columns=dataset[0])

			if len(master_data)==0:
				master_data=pd.DataFrame(master_data,columns=header)

			for key in drop_conditions.keys():
				dataset=dataset[dataset.iloc[:,key]==drop_conditions[key]]

			dataset['year']=dataset['year'].astype(int)
			dataset=dataset[dataset.iloc[:,14]>=time_conditions['start']]
			dataset=dataset[dataset.iloc[:,14]<=time_conditions['end']]
			master_data.append(dataset)

			# Save to disk.
			save_file=download_folder+'qwi_%s_%s_%s_%s_%s_u.csv' % (state,demog,firm_abrev,industry,ownership)
			print save_file
			dataset.to_csv(save_file)

			for row in dataset:
				master_data.append(row)

		except:
			pass

	master_file=download_folder+'qwi_ALL_%s_%s_%s_%s_u.csv' % (demog,firm_abrev,industry,ownership)
	master_data.to_csv(master_file)


def combine():
	"""Create an all file from many state files."""
	master_data=[]
	paths=os.listdir(download_folder)
	for path in paths:
		if path[-3:]=='csv':
			full_path=download_folder+path
			print full_path
			with open(full_path,'rb') as csvfile:
				reader=csv.reader(csvfile)
				rows=[row for row in reader]
				for row in rows[1:]:
					master_data.append(row)
	master_file=download_folder+'qwi_ALL_%s_%s_%s_%s_u.csv' % (demog,firm_abrev,industry,ownership)
	with open(master_file,'wb') as csvfile:
		writer=csv.writer(csvfile)
		for row in master_data:
			writer.writerow(row)


########################################################################################
#																					   #
#                     Functions specific to particular Projects                        #
#																					   #
########################################################################################


def create_js_dict(file):
	"""Specific to the test design of a scatterplot and two maps on codepen. Sets up the data
	object that is used in that project."""
	with open(file, 'rU') as csvfile:
		reader=csv.reader(csvfile)
		rows=[row for row in reader]

	for row in rows[1:]:
		for key in state_codes.keys():
			if state_codes[key]==str(row[1]):
				row[1]=key
		for key in ind_codes.keys():
			if row[2]==key:
				row[2]=ind_codes[key]
		row.append('Q'+str(row[5])+' '+str(row[4]))

	js_dict={}
	# Create basic data structure
	# after this it looks like:
	# {'Mining':{'Q3 2014':[],
	#            'Q4 2014':[],
	#			  etc.. }}
	for row in rows[1:]:
		wages=row[33]
		if row[2] not in js_dict.keys():
			js_dict[row[2]]={}
		if row[-1] not in js_dict[row[2]].keys():
			js_dict[row[2]][row[-1]]=[]

	for key1 in js_dict.keys():
		for key2 in js_dict[key1].keys():
			temp_rows=[]
			for row in rows:
				if row[2]==key1 and row[-1]==key2:
					temp_rows.append(row)

			temp_rows2=[row for row in temp_rows if row[6]!='' and row[33]!='' and row[11]!='' and row[14]!='']
			temp_rows=temp_rows2

			# sum up all earnbeg weighted by employment and sum employment
			w_earnbeg=0
			employment=0
			for row in temp_rows:
				employment=employment+int(row[6])
				w_earnbeg=w_earnbeg+(int(row[6])*int(row[33]))
			average_wages=w_earnbeg/employment
			average_wages=int("{:.0f}".format(average_wages))

			js_dict[key1][key2].append(average_wages)

			# Now get overall turnover - (HirA+Sep)/(2*Emp)
			numerator=0
			denominator=0
			for row in temp_rows:
				numerator=numerator+int(row[11])+int(row[14])
				denominator=denominator+(2*int(row[6]))
			average_turnover=numerator/denominator
			average_turnover=float("{:.3f}".format(average_turnover))

			js_dict[key1][key2].append(average_turnover)
			js_dict[key1][key2].append({})
			for row in temp_rows:
				wages=int(row[33])
				turnover=(int(row[11])+int(row[14]))/(2*int(row[6]))
				turnover=float("{:.3f}".format(turnover))
				js_dict[key1][key2][2][row[1]]=[wages,turnover]

	return js_dict


def professions(file,start_year,end_year):
	with open(file, 'rU') as csvfile:
		reader=csv.reader(csvfile)
		rows=[row for row in reader]

	sectors=list(set([row[6] for row in rows]))
	sector_data=[]
	for sector in sectors:
		wage_start=0
		emp_start=0
		wage_end=0
		emp_end=0
		for row in rows:
			# print row
			if row[6]==sector and row[7]==start_year and row[9]!='' and row[11]!='':
				wage_start=wage_start+int(row[11])*int(row[9])
				emp_start=emp_start+int(row[9])
			if row[6]==sector and row[7]==end_year and row[9]!='' and row[11]!='':
				wage_end=wage_end+int(row[11])*int(row[9])
				emp_end=emp_end+int(row[9])
		try:
			start=wage_start/emp_start
			end=wage_end/emp_end
			sector_data.append([sector,start,end])
		except:
			pass

	with open('/Users/aclemens/Desktop/sectors.csv','wb') as csvfile:
		writer=csv.writer(csvfile)
		for row in sector_data:
			writer.writerow(row)


def condensestates(file):
	with open(file, 'rU') as csvfile:
		reader=csv.reader(csvfile)
		rows=[row for row in reader]
	rows=rows[1:]

	sectors=list(set([row[2] for row in rows]))
	twodigits=list(set([str(row[2])[0:2] for row in rows]))
	quarters=list(set([(row[3],row[4]) for row in rows]))

	## Assemble the high/medium/low earnings lists ##
	earnings=[]
	for row in rows:
		if row[3]=='2006':
			earnings.append(row)

	earnings_dict={}
	temp=[]
	for sector in sectors:
		print sector
		temp2=[row for row in rows if row[2]==sector]
		total=0
		denom=0
		for row in temp2:
			try:
				total=total+int(row[6])*int(row[5])
				denom=denom+int(row[5])
			except:
				pass
		try:
			average=total/denom
		except:
			average=0
		earnings_dict[sector]=average
		temp.append(average)

	temp=sorted(temp)
	tertiles=int(round(len(temp)/5,0))
	tert_33=temp[tertiles]
	tert_66=temp[tertiles*4]

	for key in earnings_dict.keys():
		if earnings_dict[key]<=tert_33:
			earnings_dict[key]='low'
		elif earnings_dict[key]>tert_33 and earnings_dict[key]<=tert_66:
			earnings_dict[key]='mid'
		elif earnings_dict[key]>tert_66:
			earnings_dict[key]='high'

	print earnings_dict
	####################################################

	combined_rows=[]
	for sector in sectors:
		for quarter in quarters:
			print sector,quarter
			sector_list=[row for row in rows if row[2]==sector and row[3]==quarter[0] and row[4]==quarter[1] and row[5]!='' and row[6]!='']
			new_row=[str(sector)[0:2],sector,quarter[0],quarter[1]]

			employment=0
			earnings=0
			for row in sector_list:
				employment=employment+int(row[5])
				earnings=earnings+int(row[5])*int(row[6])

			try:
				earnings=earnings/employment
			except:
				earnings=0
			new_row.append(employment)
			new_row.append(earnings)

			combined_rows.append(new_row)

	for sector in twodigits:
		for quarter in quarters:
			sector_rows=[row for row in combined_rows if row[0]==str(sector) and row[2]==quarter[0] and row[3]==quarter[1]]
			new_row=[int(sector),'none',quarter[0],quarter[1]]

			employment=0
			earnings=0
			for row in sector_rows:
				employment=employment+int(row[4])
				earnings=earnings+int(row[4])*int(row[5])

			try:
				earnings=earnings/employment
			except:
				earnings=0
			new_row.append(employment)
			new_row.append(earnings)

			combined_rows.append(new_row)

	# Go through new list and make comparisons to the latest quarter (2014 q3)
	for sector in sectors:
		for row in combined_rows:
			if row[1]==sector and row[2]=='2014' and row[3]=='3':
				final_employ=row[4]
				final_earn=row[5]

		for row in combined_rows:
			if row[1]==sector:
				row.append(final_employ)
				row.append(final_earn)

	for sector in twodigits:
		for row in combined_rows:
			if str(row[0])==sector and str(row[2])=='2014' and row[3]=='3':
				final_employ=row[4]
				final_earn=row[5]

		for row in combined_rows:
			if str(row[0])==str(sector) and row[1]=='none':
				row.append(final_employ)
				row.append(final_earn)

	# get employment time series for each sector and attach to the appropriate combined_row
	final_rows=[row for row in combined_rows if row[2]=='2009' and row[3]=='3']
	for sector in sectors:
		year=2007
		q=4
		while year<2014 or q<=3:
			for row in final_rows:
				if str(row[1])==str(sector):
					for row2 in combined_rows:
						if row2[2]==str(year) and row2[3]==str(q) and str(row2[1])==str(sector):
							row.append(row2[4])

							if q==4:
								year=year+1
								q=1
							else:
								q=q+1

	for sector in twodigits:
		year=2007
		q=4
		while year<2014 or q<=3:
			for row in final_rows:
				if str(row[0])==str(sector) and row[1]=='none':
					for row2 in combined_rows:
						if row2[2]==str(year) and row2[3]==str(q) and str(row2[0])==str(sector) and row2[1]=='none':
							row.append(row2[4])

							if q==4:
								year=year+1
								q=1
							else:
								q=q+1

	for row in final_rows:
		for num in range(2,len(row)-1):
			row[num]=float(row[num])

	ff_rows=[]

	groups=['low','mid','high']
	for group in groups:
		temp=[]
		stotal=0
		sdenom=0
		ftotal=0
		fdenom=0
		for row in final_rows:
			if row[1] in earnings_dict.keys():
				if earnings_dict[row[1]]==group:
					stotal=stotal+row[4]*row[5]
					sdenom=sdenom+row[4]
					ftotal=ftotal+row[6]*row[7]
					fdenom=fdenom+row[6]

		print stotal,sdenom,ftotal,fdenom
		try:
			sfinal=stotal/sdenom
			ffinal=ftotal/fdenom
			growth=(ffinal-sfinal)/sfinal
		except:
			sfinal=0
			ffinal=0
			growth=0

		series=[]
		year=2007
		q=4
		while year<2014 or q<=3:
			totalemp=0
			for row in combined_rows:
				if row[1] in earnings_dict.keys():
					if row[2]==str(year) and row[3]==str(q) and earnings_dict[row[1]]==group:
						totalemp=totalemp+row[4]

			if q==4:
				year=year+1
				q=1
			else:
				q=q+1
			series.append(totalemp)

		row=[group,group,sfinal,growth,-1]
		start=series[0]
		for entry in series[1:]:
			value=(entry-start)/start
			row.append(value)

		ff_rows.append(row)

	for row in final_rows:
		flag=1
		if row[1]=='none':
			flag=0
		try:
			growth=(row[7]-row[5])/row[5]
		except:
			growth=0
		temp=[row[0],row[1],row[5],growth,flag]
		for entry in row[8:]:
			try:
				new=(entry-row[8])/row[8]
			except:
				new=0
			temp.append(new)
		ff_rows.append(temp)


	with open('/Users/aclemens/Desktop/sectors_codes.csv','rU') as csvfile:
		reader=csv.reader(csvfile)
		codes=[row for row in reader]

	for row in ff_rows:
		for code in codes:
			if str(row[1])==code[0]:
				row[1]=code[1]
			if str(row[0])==code[0]:
				row[0]=code[1]

	return ff_rows


# file='/filepath/usa_00002.csv'
# with open (file,'rU') as csvfile:
# 	reader=csv.reader(csvfile)
# 	data=[row for row in reader]
### feed data to format_acs_forinteractive

def format_acs_forinteractive(data):
	# Format of rows in data is:
	# year[0],datanum[1],serial[2],hhwt[3],statefip[4],countyfips[5],gq[6],pernum[7],perwt[8],occ2010[9],uhrswork[10],inctot[11],incwage[12]

	professions=list(set([row[0] for row in occ_codes]))
	finalrows=[]

	# construct a line for each profession that is:
	# [profession, 2006 wage, 2006 employment, 2006 hours worked, 2007 wage, 2007 employment, 2007 hours worked, ... ]
	for profession in professions:
		print 'profession',profession

		# assemble list of all rows in data that match profession
		temp_prof=[row for row in data if row[9]==profession]
		
		# start a new row that begins with the profession id
		new_row=[profession]
		
		# loop through years and find appropriate profession/year combo
		years=range(2006,2014,1)
		for year in years:
			year=str(year)
			year_rows=[row for row in temp_prof if row[0]==year]
			# these are: numerator of average wages (weighted total wages), total employment to be used as denominator,
			# numerator for usual hours worked (no longer used)
			# All three are constructed in the loop below.
			wagenum, employcount, hrsnum = 0, 0, 0

			for row in year_rows:
				wagenum=wagenum+int(row[12])*int(row[8])
				employcount=employcount+int(row[8])
				hrsnum=hrsnum+int(row[10])*int(row[8])

			try:
				new_row.append(wagenum/employcount)
			except:
				print 'no employees'
				new_row.append(0)

			new_row.append(employcount)

			try:
				new_row.append(hrsnum/employcount)
			except:
				print 'no employees'
				new_row.append(0)

		finalrows.append(new_row)

	return finalrows


def wrangle_data(data):
	# Feed format_acs_forinteractive into here.
	# format of data rows:
	# [profession, 2006 wage, 2006 employment, 2006 hours worked, 2007 wage, 2007 employment, 2007 hours worked, ... ]
	inter_rows=[]

	# First remove professions for which there are no observations in *any* single year
	data=[row for row in data if row[1]!=0 and row[2]!=0 and row[3]!=0 and row[4]!=0 and row[5]!=0 and row[6]!=0 and row[7]!=0 and row[8]!=0 and row[9]!=0 and row[10]!=0 and row[11]!=0 and row[12]!=0 and row[13]!=0 and row[14]!=0 and row[15]!=0 and row[16]!=0 and row[17]!=0 and row[18]!=0 and row[19]!=0 and row[20]!=0 and row[21]!=0 and row[22]!=0 and row[23]!=0]

	# set up low/mid/high divisions based on 2006 wages. First, build a list of wages where each
	# wage is repeated once for each weighted employed person (a leeeeetle inefficient)
	wages_2006=[]
	for row in data:
		for i in range(0,row[2],1):
			wages_2006.append(row[1])

	print len(wages_2006)
	wages_2006=sorted(wages_2006)

	# This sets up the percentiles - adjust cut1 and cut2 to get something other than the 20th and 90th
	cuts=len(wages_2006)/10
	cut1=wages_2006[int(round(cuts*2))]
	cut2=wages_2006[int(round(cuts*9))]

	# and now sort occupations into low/mid/high based on 2006 wage
	low_occs=[row for row in data if row[1]<=cut1]
	mid_occs=[row for row in data if row[1]>cut1 and row[1]<cut2]
	high_occs=[row for row in data if row[1]>=cut2]

	# quick sanity check - number of occupations in each group and the actual cuts
	print len(low_occs),len(mid_occs),len(high_occs)
	print wages_2006[int(round(cuts*2))],wages_2006[int(round(cuts*9))]

	# now create the first three rows of our eventual data structure by looping through the three
	# occupation lists and building average wage and total employment data for each. Output rows
	# for javascript are:
	# [sector, subsector, 2007 wage, wage growth, type flag, starting employment index (0),
	# emp index 2008, emp index 2009, emp index 2010...]
	for type in [['low',low_occs],['mid',mid_occs],['high',high_occs]]:
		totalemp2006, totalemp2007, totalemp2008, totalemp2009, totalemp2010, totalemp2011, totalemp2012, totalemp2013 = 0, 0, 0, 0, 0, 0, 0, 0
		average2007, average2013 = 0, 0		

		temp_row=[type[0],type[0]]

		for row in type[1]:
			average2007=average2007+row[4]*row[5]
			average2013=average2013+row[22]*row[23]

			totalemp2006=totalemp2006+row[2]
			totalemp2007=totalemp2007+row[5]
			totalemp2008=totalemp2008+row[8]
			totalemp2009=totalemp2009+row[11]
			totalemp2010=totalemp2010+row[14]
			totalemp2011=totalemp2011+row[17]
			totalemp2012=totalemp2012+row[20]
			totalemp2013=totalemp2013+row[23]

		average2007=average2007/totalemp2007
		average2013=average2013/totalemp2013

		temp_row.extend([average2007,(average2013-average2007)/average2007,-1,0])
		temp_row.extend([(totalemp2008-totalemp2007)/totalemp2007,(totalemp2009-totalemp2007)/totalemp2007,(totalemp2010-totalemp2007)/totalemp2007,(totalemp2011-totalemp2007)/totalemp2007,(totalemp2012-totalemp2007)/totalemp2007,(totalemp2013-totalemp2007)/totalemp2007])
		inter_rows.append(temp_row)

	# Adjust the last number here to screen out professions with small sample sizes
	data=[row for row in data if row[2]>250000]

	# Next create rows for each specific occupation. This is pretty easy since data is already in a
	# occupation by occupation format.
	for code in occ_codes:
		set=[row for row in data if row[0]==code[0]]
		try:
			new_row=[code[2],code[1],set[0][4],(set[0][22]-set[0][4])/set[0][4],1,0,(set[0][8]-set[0][5])/set[0][5],(set[0][11]-set[0][5])/set[0][5],(set[0][14]-set[0][5])/set[0][5],(set[0][17]-set[0][5])/set[0][5],(set[0][20]-set[0][5])/set[0][5],(set[0][23]-set[0][5])/set[0][5]]
			inter_rows.append(new_row)
		except:
			pass

	# Finally, create the aggregated occupation groupings rows
	top_occs=[row[0] for row in inter_rows]
	
	# I was running into some kind of crazy exception using set() so...
	b=[]
	for occ in top_occs:
		if occ not in b:
			b.append(occ)

	top_occs=b
	for occ in top_occs[3:]:
		occ_row=[row[0] for row in occ_codes if row[2]==occ]
		temp_occ=[row for row in data if row[0] in occ_row]
		temp_row=[occ,'none']

		totalemp2006, totalemp2007, totalemp2008, totalemp2009, totalemp2010, totalemp2011, totalemp2012, totalemp2013 = 0, 0, 0, 0, 0, 0, 0, 0
		average2007, average2013 = 0, 0

		for row in temp_occ:
			average2007=average2007+row[4]*row[5]
			average2013=average2013+row[22]*row[23]

			totalemp2006=totalemp2006+row[2]
			totalemp2007=totalemp2007+row[5]
			totalemp2008=totalemp2008+row[8]
			totalemp2009=totalemp2009+row[11]
			totalemp2010=totalemp2010+row[14]
			totalemp2011=totalemp2011+row[17]
			totalemp2012=totalemp2012+row[20]
			totalemp2013=totalemp2013+row[23]

		average2007=average2007/totalemp2007
		average2013=average2013/totalemp2013

		temp_row.extend([average2007,(average2013-average2007)/average2007,0,0])
		temp_row.extend([(totalemp2008-totalemp2007)/totalemp2007,(totalemp2009-totalemp2007)/totalemp2007,(totalemp2010-totalemp2007)/totalemp2007,(totalemp2011-totalemp2007)/totalemp2007,(totalemp2012-totalemp2007)/totalemp2007,(totalemp2013-totalemp2007)/totalemp2007])
		inter_rows.append(temp_row)

	# Just a little formatting to create a nice small javascript object
	for i,row in enumerate(inter_rows):
		row[0]=row[0].lower().title()
		for j,entry in enumerate(row):
			try:
				inter_rows[i][j]=round(entry,2)
			except:
				pass

	inter_rows[0][0]=inter_rows[0][0].lower()
	inter_rows[1][0]=inter_rows[1][0].lower()
	inter_rows[2][0]=inter_rows[2][0].lower()

	return inter_rows








