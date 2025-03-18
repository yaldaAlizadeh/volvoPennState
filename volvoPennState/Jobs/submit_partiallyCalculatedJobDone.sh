#!/bin/bash
#SBATCH --nodes=20
#SBATCH --ntasks=10
#SBATCH --mem=64GB
#SBATCH --time=47:00:00
#SBATCH --account=vuh14_dibbs_sc
#SBATCH --partition="sla-prio"
#SBATCH --output="Jobs/outputs2/outputForJob_%j.txt"
#SBATCH --error="Jobs/errors2/errorFileName_%j.txt"
#SBATCH --job-name="checkPartReplacementVolvoPennStateCollab-$1"



module load anaconda3
echo "starting to run fixPartReplacement for job id="$SLURM_JOB_ID
export XDG_RUNTIME_DIR=""

jupyter nbconvert --to script /storage/home/yqf5148/work/volvoPennState/CalculateFeaturesForThisVIN-fromWhereItLeftOff.ipynb.ipynb
#above line does not work. in order to generate .pyb file from ipynb file, simply we go to file-> Save and Export Notebook as - > Executable Script

echo "the VIN for this Job is: " $1
ipython /storage/home/yqf5148/work/volvoPennState/CalculateFeaturesForThisVIN-fromWhereItLeftOff.py $1 $2 $SLURM_JOB_ID

echo "Finish running fixPartReplacement."


