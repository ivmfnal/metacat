import os,time,sys,datetime, glob, fnmatch,string,subprocess, json

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')

month = sys.argv[1]
first = sys.argv[1]
last = sys.argv[2]

out = open("report_"+first+"_"+last+".tex",'w')
top = "\\documentclass[10pt]{article}\n"
top += "\\setlength{\\textwidth}{6.5in}\n"
top += "\\setlength{\\oddsidemargin}{0.00in}"
top += "\\begin{document} \n"
begin = "\\begin{table}\n\\begin{tabular}{rrrrrrrr}\n"
out.write(top)
out.write(begin)
header = "Expt& version& tier& stream&files& events& size, TB& event size, MB\\\\\n"
out.write(header)
for expt in ["protodune-sp","protodune-dp"]:
  for stream in ["physics","cosmics","test","commissioning","ALL"]:
    for tier in ["raw","full-reconstructed","pandora_info","hit-reconstructed","ALL"]:
#for expt in ["protodune-dp"]:
#  for stream in ["cosmics","ALL"]:
#    for tier in ["full-reconstructed"]:
      for version in ["v08ALL","raw"]:
        if (tier == "raw" and version != "raw") or (tier != "raw" and version == "raw" ):
          continue
        
        command = ""
        command += "data_tier "+tier
        command += " and data_stream " + stream
        command += " and run_type " + expt
        command += " and file_type detector "
        command += " and create_date >= " + first
        command += " and create_date <= " + last
        if tier != "raw":
          command += " and version " + version
        command = command.replace("ALL","%")
        print (command)
        result = samweb.listFilesSummary(command)
        #print result
        
        file_count = result["file_count"]
        if file_count == 0:
          continue
        events = result["total_event_count"]
        ssize = result["total_file_size"]/1000/1000/1000/1000.
        fsize = ssize/events*1000*1000
        #print (expt,version,tier,stream,file_count,events,ssize," TB",fsize," MB")
        data = "%s& %s& %s& %s& %s& %d& %10.1f& %10.1f\\\\\n"%(expt,version,tier,stream,file_count,events,ssize,fsize)
        data = data.replace("_","$\_$")
        data = data.replace("%","ALL")
        print (data)
        out.write(data)

end = "\\end{tabular}\n"
out.write(end)
end = "\\caption{Summary of data %s to %s}\n\\end{table}\n"%(first,last)
out.write(end)

#out.close()

#out = open("report_"+first+"_"+last+".tex",'w')
top = "\\begin{table}\n\\begin{tabular}{rrrrrrrr}\n"
out.write(top)
header = "Expt&type&version&tier&files&events&size(TB)&size(MB)\\\\\n"
out.write(header)
for expt in ["protodune-sp","protodune-dp","fardetALL","neardetALL","ALL"]:
  for version in ["v07ALL","v08ALL","raw","ALL"]:
    for tier in ["simulated","detector-simulated","full-reconstructed","pandora_info","ALL",]:
      command = "version " + version
      command += " and run_type "+expt
      command += " and file_type mc "
      command += " and data_tier "+tier
      #command += " and data_stream " + stream
      #command += " and run_type " + expt
      command += " and create_date >= " + first
      command += " and create_date <= " + last
      #print "%"+command
      command = command.replace("ALL","%")
      print (command)
      result = samweb.listFilesSummary(command)
      print (result)
      
      file_count = result["file_count"]
      if file_count == 0:
        continue
      events = result["total_event_count"]
      ssize = result["total_file_size"]/1000/1000/1000/1000.
      if events == None:
        events = 0
        fsize = 0.0
      else:
        fsize = ssize/events*1000*1000
      
      print (expt,version,tier,events,ssize," TB")
      data = "%s & mc & %s& %s &%s &%d &%10.1f&%10.1f\\\\\n"%(expt,version,tier,file_count,events,ssize,fsize)
      data = data.replace("_","$\_$")
      out.write(data)
end = "\\end{tabular}\n"
out.write(end)
end = "\\caption{Summary of mc production %s to %s}\n\\end{table}"%(first,last)
out.write(end)
out.write("\\end{document}\n")
out.close()
    
