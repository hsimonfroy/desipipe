from desipipe import Queue, Environment, TaskManager, FileManager, setup_logging

setup_logging()

queue = Queue('y1_abacus_first_gen_cutsky')
environ = Environment('nersc-cosmodesi')

tm = TaskManager(queue=queue, environ=environ)

tm_compute = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=64))
tm_plot = tm.clone(scheduler=dict(max_workers=1), provider=dict(provider='nersc', mpiprocs_per_worker=1))


@tm_compute.python_app
def compute_power(data, randoms, output):
    from pypower import CatalogFFTPower
    from cosmoprimo.fiducial import DESI
    data = data.read()
    randoms = randoms.read()
    cosmo = DESI()
    data_positions = [data['RA'], data['DEC'], cosmo.comoving_radial_distance(data['Z'])]
    randoms_positions = [randoms['RA'], randoms['DEC'], cosmo.comoving_radial_distance(randoms['Z'])]
    power = CatalogFFTPower(data_positions1=data_positions, randoms_positions1=randoms_positions,
                            data_weights1=data['WEIGHT'], randoms_weights1=randoms['WEIGHT'],
                            position_type='rdd', edges={'step': 0.001}, ells=(0, 2, 4), los='firstpoint',
                            boxsize=10000., nmesh=1024, resampler='tsc', interlacing=3).poles
    output.write(power)
    return power


@tm_plot.python_app
def plot_power(powers):
    import numpy as np
    from matplotlib import pyplot as plt
    ax = plt.gca()
    for ell in powers[0].ells:
        mean = np.mean([power(ell=ell, complex=False) for power in powers])
        ax.plot(powers[0].k, powers[0].k * mean, label=r'\ell = {:d}'.format(ell))
    plt.savefig('tmp.png')


if __name__ == '__main__':

    fm = FileManager('files/y1_first_gen.yaml', environ=environ).select('abacus cutsky')
    outputs = []
    for fi in fm:
         # save metadata: python and slurm scripts in the output.base_dir directory
        outputs.append(compute_power(fi.get(filetype='catalog', keywords='data'), fi.get(filetype='catalog', keywords='randoms'), fi.get(filetype='power')))
    plot_power(outputs)
