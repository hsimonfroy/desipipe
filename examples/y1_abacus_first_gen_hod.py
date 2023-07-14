from desipipe import Queue, Environment, TaskManager, FileManager, setup_logging

setup_logging()

environ = Environment('nersc-cosmodesi')
queue = Queue('y1_abacus_first_gen_hod')
tm = TaskManager(queue=queue, environ=environ)

tm_power = tm.clone(scheduler=dict(max_workers=5), provider=dict(provider='nersc', mpiprocs_per_worker=64, time='00:15:00'))
#tm_power = tm.clone(scheduler=dict(max_workers=1), provider='local')
#tm_power = tm.clone(scheduler=dict(max_workers=1), provider=dict(provider='local', mpiprocs_per_worker=64, mpiexec='srun -n {mpiprocs:d} {cmd}')) 
tm_profile = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=6, nodes_per_worker=0.2, time='00:10:00'))
tm_sample = tm.clone(scheduler=dict(max_workers=10), provider=dict(provider='nersc', mpiprocs_per_worker=32, nodes_per_worker=0.5, time='01:00:00'))


def get_template(template_name='standard', z=0.8, klim=None):

    import numpy as np

    """A simple wrapper that returns the template of interest."""

    from desilike.theories.galaxy_clustering import (StandardPowerSpectrumTemplate, ShapeFitPowerSpectrumTemplate,
                                                     WiggleSplitPowerSpectrumTemplate,BandVelocityPowerSpectrumTemplate, DirectPowerSpectrumTemplate, BAOPowerSpectrumTemplate)

    if 'standard' in template_name:
        template = StandardPowerSpectrumTemplate(z=z)
    elif 'shapefit' in template_name:
        template = ShapeFitPowerSpectrumTemplate(z=z)
    elif 'wigglesplit' in template_name:
        template = WiggleSplitPowerSpectrumTemplate(z=z)
    elif 'ptt' in template_name:
        template = BandVelocityPowerSpectrumTemplate(z=z, kp=np.arange(*klim))
    elif 'direct' in template_name:
        template = DirectPowerSpectrumTemplate(z=z)
        template.params['omega_b'].update(fixed=False, prior={'dist': 'norm', 'loc': 0.02237, 'scale': 0.00037})
        template.params['n_s'].update(fixed=True)
    elif 'bao' in template_name:
        template = BAOPowerSpectrumTemplate(z=z)
    return template


def get_theory(theory_name='velocileptors', observable_name='power', b1E=1.9, template=None, ells=(0, 2, 4)):

    """A simple wrapper that returns the theory of interest."""

    from desilike.theories.galaxy_clustering import (LPTVelocileptorsTracerPowerSpectrumMultipoles, LPTVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     LPTMomentsVelocileptorsTracerPowerSpectrumMultipoles, LPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     EPTMomentsVelocileptorsTracerPowerSpectrumMultipoles, EPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles,
                                                     PyBirdTracerPowerSpectrumMultipoles, PyBirdTracerCorrelationFunctionMultipoles,
                                                     DampedBAOWigglesTracerPowerSpectrumMultipoles, DampedBAOWigglesTracerCorrelationFunctionMultipoles)

    kwargs = {}
    euler = False
    if 'bird' in theory_name:
        euler = True
        kwargs.update(eft_basis='westcoast')
        Theory = PyBirdTracerPowerSpectrumMultipoles if observable_name == 'power' else PyBirdTracerCorrelationFunctionMultipoles
    elif 'velo' in theory_name:
        Theory = LPTVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else LPTVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'lptm' in theory_name:
        Theory = LPTMomentsVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else LPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'eptm' in theory_name:
        euler = True
        Theory = EPTMomentsVelocileptorsTracerPowerSpectrumMultipoles if observable_name == 'power' else EPTMomentsVelocileptorsTracerCorrelationFunctionMultipoles
    elif 'dampedbao' in theory_name:
        euler = True
        Theory = DampedBAOWigglesTracerPowerSpectrumMultipoles if observable_name == 'power' else DampedBAOWigglesTracerCorrelationFunctionMultipoles

    theory = Theory(template=template, **kwargs)
    # Changes to theory.init.params will remain whatever pipeline is built
    b1 = float(euler) + b1E - 1.
    theory.init.params['b1'].update(value=b1, ref={'limits': [b1 - 0.1, b1 + 0.1]})
    for param in theory.init.params.select(basename=['alpha6']): param.update(fixed=True)
    if 4 not in ells:
        for param in theory.init.params.select(basename=['alpha4', 'sn4*', 'al4_*']): param.update(fixed=True)
    if observable_name != 'power':
        for param in theory.init.params.select(basename=['ce1', 'sn0', 'al0_0']): param.update(fixed=True)
    return theory


def get_fit_setup(tracer, ells=None, theory_name='velocileptors'):
    if ells is None:
        ells = (0, 2, 4)
    if 'bao' in theory_name: ells = (0, 2)
    if tracer.startswith('LRG'):
        z = 0.8
        b0 = 1.7
        smin, kmax = 30., 0.2
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    if tracer.startswith('ELG'):
        z = 1.1
        b0 = 0.84
        smin, kmax = 25., 0.2
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    if tracer.startswith('QSO'):
        z = 1.4
        b0 = 1.2
        smin, kmax = 20., 0.25
        if 'bao' in theory_name: smin, kmax = 40., 0.3
        klim = {ell: [0.02, kmax, 0.005] for ell in ells}
        slim = {ell: [smin, 150., 4.] for ell in ells}
    return z, b0, klim, slim


def get_observable_likelihood(data=None, covariance=None, wmatrix=None, theory_name='velocileptors', ells=(0, 2, 4), template_name='shapefit', observable_name='power', tracer='LRG', solve=True, save_emulator=False, emulator_fn=None, mpicomm=None):

    """Return the power spectrum likelihood, optionally computing the emulator (if ``save_emulator``)."""

    from desilike.observables.galaxy_clustering import TracerPowerSpectrumMultipolesObservable, TracerCorrelationFunctionMultipolesObservable
    from desilike.likelihoods import ObservablesGaussianLikelihood

    z, b0, klim, slim = get_fit_setup(tracer, ells=ells, theory_name=theory_name)
    print(z, klim, slim)

    from cosmoprimo.fiducial import DESI
    fiducial = DESI()
    b1E = b0 / fiducial.growth_factor(z)

    # Load theory
    theory = get_theory(theory_name=theory_name, observable_name=observable_name, template=None, b1E=b1E, ells=klim.keys())
    if 'bao' in theory_name:
        if save_emulator:
            raise ValueError('No need to build an emulator for the BAO model!')
        emulator_fn = None

    if save_emulator or emulator_fn is None:  # No emulator available (yet)
        template = get_template(template_name=template_name, z=z, klim=(klim[0][0], klim[0][1] + 1e-5, klim[0][2]))
        theory.init.update(template=template)
    else:  # Load emulator
        from desilike.emulators import EmulatedCalculator
        calculator = EmulatedCalculator.load(emulator_fn)
        theory.init.update(pt=calculator)

    if observable_name == 'power':
        observable = TracerPowerSpectrumMultipolesObservable(klim=klim, data=data, covariance=covariance, wmatrix=wmatrix, kinlim=(0., 0.25), theory=theory)

    if observable_name == 'corr':
        observable = TracerCorrelationFunctionMultipolesObservable(slim=slim, data=data, covariance=covariance, theory=theory)

    likelihood = ObservablesGaussianLikelihood(observables=[observable], scale_covariance=1. / 25.)  # likelihood is a callable that returns the log-posterior
    if mpicomm is not None:
        likelihood.mpicomm = mpicomm
    
    if save_emulator:  # Compute and save emulator
        likelihood()  # to set up k-ranges for the emulator
        from desilike.emulators import Emulator, TaylorEmulatorEngine
        emulator = Emulator(theory.pt, engine=TaylorEmulatorEngine(method='finite', order=4))
        emulator.set_samples()
        emulator.fit()
        emulator.save(emulator_fn)
    # likelihood.all_params gives access to the parameters of the likelihood pipeline
    if solve:
        for param in likelihood.all_params.select(name=['alpha*', 'sn*', 'c*']):
            if param.varied: param.update(derived='.auto')
        for param in likelihood.all_params.select(name=['al*']):
            if param.varied: param.update(prior=None, derived='.auto')
    if likelihood.mpicomm.rank == 0:
        likelihood.log_info('Use analytic marginalization for {}.'.format(likelihood.all_params.names(solved=True)))

    return likelihood


@tm_power.python_app
def compute_power(data, output):
    import numpy as np
    from mockfactory import utils
    from pypower import CatalogFFTPower, setup_logging
    from cosmoprimo.fiducial import DESI
    
    setup_logging()

    cosmo = DESI()
    for dd in data:
        z = float(dd.get().options['z'])
        break
    a = 1. / (1. + z)
    E = cosmo.efunc(z)
    data = [data.get().read() for data in data]
    data = data[0].concatenate(data)
    velocities = np.column_stack([data[name] for name in ['vx', 'vy', 'vz']]) / (100. * a * E)
    if 'z_rsd' in data:
        data['z'] = data['z_rsd'] - velocities[:, 2]
    else: # ihod 7, 8
        data['z'] = data['z'] - velocities[:, 2]
    positions = np.column_stack([data[name] for name in ['x', 'y', 'z']])
    los = output.options['los']
    vlos = [1. * (los == axis) for axis in 'xyz']
    positions += utils.vector_projection(velocities, vlos)
    power = CatalogFFTPower(data_positions1=positions, position_type='pos', edges={'step': 0.001},
                            boxsize=2000., boxcenter=1000., nmesh=512, resampler='tsc', interlacing=3,
                            los=los, wrap=True, mpiroot=None)
    power.poles.attrs['z'] = power.attrs['z'] = z
    if power.mpicomm.rank == 0:
        output.write(power)
    return output


def plot_power(data):
    from matplotlib import pyplot as plt
    ax = plt.gca()
    for data in data:
        ihod = data.options['ihod']
        data = data.read().select((0., 0.3, 0.01))
        for ill, ell in enumerate((0, 2, 4)):
            k, pk = data(ell=ell, return_k=True, complex=False)
            ax.plot(k, k * pk, color='C{:d}'.format(ill), alpha=0.1)
    ax.set_xlabel(r'$k$ [$h/\mathrm{Mpc}$]')
    ax.set_ylabel(r'$k P(k)$ [$(\mathrm{Mpc}/h)^2$]')
    ax.legend()
    plt.savefig('plot_hod{:d}.png'.format(ihod))
    plt.close(plt.gcf())


@tm_power.python_app
def compute_window(power, output):
    import numpy as np
    from pypower import MeshFFTWindow
    power = power.read()
    edgesin = np.linspace(0., 0.5, 501)
    window = MeshFFTWindow(edgesin=edgesin, power_ref=power, periodic=True)
    if window.mpicomm.rank == 0:
        output.write(window.save)
    return output


@tm_sample.python_app
def emulate(data, wmatrix, get_observable_likelihood=get_observable_likelihood, **kwargs):
    get_observable_likelihood(data=[dd.read() for dd in data], wmatrix=wmatrix.filepath, save_emulator=True, **kwargs)


@tm_profile.python_app
def profile(data, wmatrix, output, get_observable_likelihood=get_observable_likelihood, **kwargs):
    from desilike.profilers import MinuitProfiler
    ihod, ells = data[0].options['ihod'], output.foptions['ells']
    print([dd.filepath for dd in data])
    likelihood = get_observable_likelihood(data=[dd.read() for dd in data], wmatrix=wmatrix.filepath, save_emulator=False, ells=output.options['ells'], **kwargs)
    profiler = MinuitProfiler(likelihood, seed=42)
    profiles = profiler.maximize(niterations=4)
    profiles = profiler.interval(['qpar', 'qper', 'df', 'dm'])
    likelihood(**profiles.bestfit.choice(input=True))
    likelihood.observables[0].plot(fn='plot_bestfit_power_ells{}_HOD{:d}.png'.format(ells, ihod))
    output.write(profiles.save)
    return output

    
def plot_profiles(*list_profiles):
    from desilike.samples import Profiles, plotting
    labels = ['$\ell = {}$'.format(', '.join([str(ell) for ell in profiles[0].options['ells']])) for profiles in list_profiles]
    list_profiles = [[profile.read(Profiles.load).choice() for profile in profiles] for profiles in list_profiles]  # choice() to keep best fit
    for profiles in list_profiles:
        for profile in profiles:
            print(profile.to_stats(tablefmt='pretty'))
    from desilike.samples import plotting
    params = ['qpar', 'qper', 'df', 'dm']
    truths = [1., 1., 1., 0.]
    profiles = [Profiles(bestfit=list_profiles[0][0].bestfit.concatenate([profiles[ip].bestfit.select(name=params) for profiles in list_profiles]),
                         error=list_profiles[0][0].error.concatenate([profiles[ip].error.select(name=params) for profiles in list_profiles])) for ip in range(len(list_profiles[0]))]
    for profile in profiles:
        print(len(profile.bestfit))
    plotting.plot_aligned_stacked(profiles, params=params, truths=truths, labels=labels,
                                  ids=['HOD{:d}'.format(i) for i in range(1, 9)], fn='plot_bestfit_params.png')
    
    
@tm_sample.python_app
def sample(data, wmatrix, output, get_observable_likelihood=get_observable_likelihood, resume=False, **kwargs):
    from desilike.samplers import EmceeSampler
    likelihood = get_observable_likelihood(data=[dd.read() for dd in data], wmatrix=wmatrix.filepath, save_emulator=False, ells=output.options['ells'], **kwargs)
    save_fn = output.filepaths
    chains = None
    if resume: chains = save_fn
    sampler = EmceeSampler(likelihood, chains=chains, nwalkers=40, seed=42, save_fn=save_fn)
    sampler.run(min_iterations=2000, check={'max_eigen_gr': 0.03})


if __name__ == '__main__':

    fm = FileManager('files/y1_first_gen.yaml', environ=environ)
    tracer = 'LRG'
    fm_abacus = fm.select(keywords='abacus box hod', tracer=tracer)
    data = {}
    for ihod in range(9):
        data[ihod] = []
        for fi in fm_abacus.select(filetype=['catalog', 'power'], ihod=ihod):
            for los in ['x', 'y', 'z']:
                file = fi.get(filetype='power', los=los)
                file = compute_power(fi.select(filetype='catalog'), file)
                data[ihod].append(file)

    #for ihod, data in data.items():
    #    plot_power(data)
    
    wm_fn = fm.get(keywords='box 2gpc window')
    wm_fn = compute_window(data[1][0], wm_fn)
    
    fm_ez = fm.select(keywords=['y1 ez box'], tracer=tracer)
    emulator_fn = '_emulators/emulator.npy'
    kwargs = dict(covariance=fm_ez.filepaths,
                  theory_name='velocileptors',
                  template_name='shapefit', observable_name='power',
                  tracer=tracer, emulator_fn=emulator_fn)
    emulate(data[1], wm_fn, **kwargs)
    list_profiles = []
    for ells in [(0, 2), (0, 2, 4)]:
        profiles = []
        for ihod, dd in data.items():
            file = fm_abacus.get(keywords='profile', ihod=ihod, ells=ells)
            file = profile(dd, wm_fn, file, **kwargs)
            profiles.append(file)
            sample(data, wm_fn, fm_abacus.select(keywords='chain', ihod=ihod, ells=ells), **kwargs)
        list_profiles.append(profiles)
    plot_profiles(*list_profiles)
    