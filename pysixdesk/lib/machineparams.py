# LHC settings
LHC = {}
# TODO: fill these in.
# defaults
_LHC_DEF = dict(phi_IR1=90.000,
                phi_IR5=0.000,
                b_t_dist=25.,  # [ns]
                # bunch_charge=2.2e11,
                # emit_norm_x=2.5,  # [um]
                # emit_norm_y=2.5,  # [um]
                int_tune_x=62.0,
                int_tune_y=60.0,
                q_prime_x=3.0,
                q_prime_y=3.0,
                fraction_crab=0,
                seed_ran=1,
                lhc_beam=1  # 2: b2 clockwise, 4: b2 counter clockewise
                )
# injection defaults
_LHC_INJ = dict(rf_vol=8.0,  # [MV]
                sig_z=0.11,  # [m]
                sig_e=4.5e-04,
                e_0=450000.0,  # [MeV]
                # i_mo= -40,  # [A]
                tune_x=62.28,
                tune_y=60.31,
                )
# collision defaults
_LHC_COL = dict(rf_vol=16.0,  # [MV]
                sig_z=0.77e-1,  # [m]
                sig_e=1.1e-04,
                e_0=7000000.0,  # [MeV]
                # i_mo= ,  # [A]
                tune_x=62.31,
                tune_y=60.32,
                )
LHC['inj'] = {**_LHC_DEF, **_LHC_INJ}
LHC['col'] = {**_LHC_DEF, **_LHC_COL}

# HLLHC settings
HLLHC = {}
# defaults
_HLLHC_DEF = dict(phi_IR1=90.0,  # flat optics: 0.0
                  phi_IR5=0.0,  # flat optics: 90.0
                  b_t_dist=25.,  # [ns]
                  bunch_charge=2.2e11,
                  emit_norm_x=2.5,  # [um]  # These need to be norm
                  emit_norm_y=2.5,  # [um]
                  int_tune_x=62.0,
                  int_tune_y=60.0,
                  q_prime_x=3.0,
                  q_prime_y=3.0,
                  fraction_crab=0,
                  seed_ran=1,
                  lhc_beam=1  # 2: b2 clockwise, 4: b2 counter clockewise
                  )
# injection defaults
_HLLHC_INJ = dict(rf_vol=8.0,  # [MV]
                  sig_z=0.130,  # [m]
                  sig_e=4.5e-04,
                  e_0=450000.0,  # [MeV]
                  i_mo=-20,  # [A] ??? is this correct ?
                  tune_x=62.28,
                  tune_y=60.31,
                  )
# collision defaults
_HLLHC_COL = dict(rf_vol=16.0,  # [MV]
                  sig_z=0.075,  # [m]
                  sig_e=1.1e-4,
                  e_0=7000000.0,  # [MeV]
                  i_mo=-570,  # [A] ??? is this correct ?
                  tune_x=62.31,
                  tune_y=60.32,
                  )
HLLHC['inj'] = {**_HLLHC_DEF, **_HLLHC_INJ}
HLLHC['col'] = {**_HLLHC_DEF, **_HLLHC_COL}
