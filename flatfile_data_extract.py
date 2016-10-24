#!/usr/bin/env python

"""
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%% Omicron .Flat File Analysis Functions %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
"""

import Tkinter as Tkinter
from Tkinter import Tk
from tkFileDialog import askopenfilename
from tkFileDialog import askdirectory
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import leastsq
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.patches as patches

import flatfile

"""
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%            Data Importing             %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
"""

def locate_data():
    Tkinter.Tk().withdraw()
    file_name = askopenfilename()
    return file_name

def import_data(file_name):
    file_data = flatfile.load(file_name)
    return file_data

def extract_data_type(file_data):
    data_type = file_data[0].info['type']
    return data_type

def extract_file_date(file_data):
    file_date = file_data[0].info['date']
    return file_date

def extract_scan_directions(file_data):
    scan_dimensions = len(file_data)
    scan_directions = []
    for i in range(scan_dimensions):
        scan_directions.append(file_data[i].info['direction'])
    return scan_directions

"""
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%            CITS Functions             %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
"""

def extract_cits_data(file_data):
    cits_data = []
    for i in range(0, len(file_data)):
        cits_data.append(file_data[i].data)
    return cits_data

def extract_cits_info(file_data):
    cits_info = {}
    for i in range(0, len(file_data)):
        cits_info['location'] = file_data[i].info['filename']
        cits_info['filename'] = cits_info['location'].split('/')
        cits_info['filename'] = cits_info['filename'][len(cits_info['filename'])-1]
        cits_info['comment'] = file_data[i].info['comment']
        cits_info['date'] = file_data[i].info['date']
        cits_info['type'] = file_data[i].info['type']
        cits_info['xres'] = file_data[i].info['xres']
        cits_info['yres'] = file_data[i].info['yres']
        cits_info['xinc'] = file_data[i].info['xinc']
        cits_info['yinc'] = file_data[i].info['yinc']
        cits_info['xreal'] = file_data[i].info['xreal']
        cits_info['yreal'] = file_data[i].info['yreal']
        cits_info['unitxy'] = file_data[i].info['unitxy']
        cits_info['vres'] = file_data[i].info['vres']
        cits_info['vstart'] = file_data[i].info['vstart']
        cits_info['vinc'] = file_data[i].info['vinc']
        cits_info['vreal'] = file_data[i].info['vreal']
        cits_info['unitv'] = file_data[i].info['unitv']
        cits_info['vgap'] = file_data[i].info['vgap']
        cits_info['iset'] = file_data[i].info['current']
    return cits_info

def plot_CITS_energy(file_data, energy_slice, scan_dir=0, color_map='hot', font_size=18, font_weight='bold'):
    cits_window = plt.figure()
    cits_image = cits_window.add_subplot(111)

    cits_info = extract_cits_info(file_data)
    x_extent = cits_info['xres']*cits_info['xinc']
    y_extent = cits_info['yres']*cits_info['yinc']
    v_range = np.arange(cits_info['vstart'], cits_info['vres']*cits_info['vinc'], cits_info['vinc'])

    cits_data = extract_cits_data(file_data)

    cits_image_cbarCall = cits_image.imshow(cits_data[scan_dir][energy_slice], origin='lower', extent=[0, x_extent, 0, y_extent], cmap=color_map, interpolation='none')
    cbar = cits_window.colorbar(cits_image_cbarCall)
    cits_image.set_xlabel(cits_info['unitxy'], fontsize=font_size, fontweight=font_weight)
    cits_image.set_ylabel(cits_info['unitxy'], fontsize=font_size, fontweight=font_weight)
    cits_image.set_title(str(np.round(v_range[energy_slice], decimals=3))+cits_info['unitv'])
    cbar.set_label(cits_info['unitv'], fontsize=font_size, fontweight=font_weight)
    plt.show()


class plot_cits():

    # Calling plot_cits() requires:
    # file_data - data extracted from load_cits.import_data() function

    # options:
    # initialslice - Define the initial energy 'slice' to plot. Default is vstart
    # scan_dir - If the CITS has multiple scan directions (i.e. up-retrace or down-trace etc.). Default is zero (fwd-up-retrace)
    # color_map - Choose color map of images. Default is 'hot'
    # font_size - Define font size of labels.
    # font_weight - Define style of font.
    def __init__(self, file_data, initialslice=0, scan_dir=0, color_map='hot', font_size=18, font_weight='bold'):
        self.file_data = file_data

        # Create a matplotlib Figure to populate with cits image and colour bar.
        self.cits_window = plt.Figure()

        # Add a subplot to take the cits image
        self.cits_image = self.cits_window.add_subplot(111)

        # Extract the cits info from the data
        self.cits_info = extract_cits_info(file_data)
        self.x_extent = self.cits_info['xres']*self.cits_info['xinc']
        self.y_extent = self.cits_info['yres']*self.cits_info['yinc']
        self.v_start = self.cits_info['vstart']
        self.v_inc = self.cits_info['vinc']
        self.v_res = self.cits_info['vres']
        self.v_end = self.v_start + (self.v_res*self.v_inc)
        self.v_range = np.arange(self.v_start, self.v_start + (self.v_res*self.v_inc), self.v_inc)
        self.v_len = len(self.v_range)

        # Extract the x,y,z data fom the file_data
        self.cits_data = extract_cits_data(file_data)

        # Produce the initial 2D CITS plot at the initial voltage
        self.cits_image_cbarCall = self.cits_image.imshow(self.cits_data[scan_dir][initialslice], origin='lower', extent=[0, self.x_extent, 0, self.y_extent], cmap=color_map, interpolation='none')
        self.cbar = self.cits_window.colorbar(self.cits_image_cbarCall)
        self.cits_image.set_xlabel(self.cits_info['unitxy'], fontsize=font_size, fontweight=font_weight)
        self.cits_image.set_ylabel(self.cits_info['unitxy'], fontsize=font_size, fontweight=font_weight)
        self.cits_image.set_title(str(np.round(self.v_range[initialslice], decimals=3))+self.cits_info['unitv'])
        self.cbar.set_label(self.cits_info['unitv'], fontsize=font_size, fontweight=font_weight)


        # Construct a Tkinter window to place CITS plot and other components
        self.cits_viewer = Tkinter.Tk()
        self.cits_canvas = FigureCanvasTkAgg(self.cits_window, master=self.cits_viewer)
        self.cits_canvas.get_tk_widget().grid(column=0, row=1, columnspan=3)
        self.cid = self.cits_canvas.mpl_connect('button_press_event', self.mouse_click)


        # Label of the data filename
        self.data_label = Tkinter.Label(self.cits_viewer, text='None', bg='blue', fg='white')
        self.data_label.configure(text=self.cits_info['filename'])
        self.data_label.grid(column=0, row=0, columnspan=3, sticky='EW')

        # A slider (Tkinter.Scale) that controls the voltage slice currently shown
        self.cits_slider = Tkinter.Scale(master=self.cits_viewer, from_=0, to=self.v_len-1, orient=Tkinter.HORIZONTAL, command=self.return_slider_val, length=250)
        self.cits_slider.grid(column=0, row=2, columnspan=3, sticky='S')

        # Button to extract a point spectra from the CITS data
        self.cits_pointspec_button = Tkinter.Button(master=self.cits_viewer, text=u'Extract Point Spectrum', command=self.extract_pointspectrum)
        self.cits_pointspec_button.grid(column=0, row=3, sticky='W')

        # Label showing currently selected pixel
        self.x_loc = 0
        self.y_loc = 0
        self.cits_pointspec_label = Tkinter.Label(self.cits_viewer, text='None', bg='white', fg='black')
        self.cits_pointspec_label.configure(text='x: '+str(self.x_loc)+'  y: '+str(self.y_loc))
        self.cits_pointspec_label.grid(column=0, row=3, sticky='E')

        # Save currently displayed CITS energy slice
        self.cits_save_button = Tkinter.Button(master=self.cits_viewer, text=u'Save CITS', command=self.save_button)
        self.cits_save_button.grid(column=0, row=4, sticky='W')

        # Cleanly exit the CITS viewer
        self.cits_quit_button = Tkinter.Button(master=self.cits_viewer, text=u'Close CITS', command=self.quit_button)
        self.cits_quit_button.grid(column=2, row=4, sticky='E')

        # Define Window Geometry
        self.screen_w = self.cits_viewer.winfo_screenwidth()
        self.screen_h = self.cits_viewer.winfo_screenheight()
        self.cits_viewer.grid_columnconfigure(0, weight=1)
        self.cits_viewer.resizable(True, True)
        self.cits_viewer.geometry('%dx%d+%d+%d' % (0.41*self.screen_w, 0.69*self.screen_h, 0.05*self.screen_w, 0.05*self.screen_h))
        self.cits_viewer.update_idletasks()
        self.cits_canvas.show()

        self.is_rectangle = 0

        # Run cits_viewer
        Tk.mainloop(self.cits_viewer)

    def mouse_click(self, event):
        self.x_res_graph = self.cits_info['xinc']
        self.y_res_graph = self.cits_info['yinc']
        self.x_inst = event.xdata
        self.y_inst = event.ydata
        if isinstance(event.xdata, float):
            if isinstance(event.ydata, float):
                self.x_loc = int(self.x_inst / self.x_res_graph)
                self.y_loc = int(self.y_inst / self.y_res_graph)
                self.cits_pointspec_label.configure(text='x: '+str(self.x_loc)+'  y: '+str(self.y_loc))
                if self.is_rectangle == 1:
                    self.point_rectangle.remove()
                    self.point_rectangle = self.cits_image.add_patch(patches.Rectangle((self.x_loc*self.x_res_graph, self.y_loc*self.y_res_graph), self.x_res_graph, self.y_res_graph))
                else:
                    self.point_rectangle = self.cits_image.add_patch(patches.Rectangle((self.x_loc*self.x_res_graph, self.y_loc*self.y_res_graph), self.x_res_graph, self.y_res_graph))
        else:
            self.x_loc = 0
            self.y_loc = 0
        self.is_rectangle = 1
        self.cits_canvas.show()

    def update_CITS_energy(self, energy_slice, scan_dir=0, colour_map='hot'):

        self.cits_image.imshow(self.cits_data[scan_dir][energy_slice], origin='lower', extent=[0, self.x_extent, 0, self.y_extent], cmap=colour_map, interpolation='none')
        self.img_min = np.amin(self.cits_data[scan_dir][energy_slice])
        self.img_max = np.amax(self.cits_data[scan_dir][energy_slice])
        self.cits_image.set_title(str(np.round(self.v_range[energy_slice], decimals=3))+' V')
        self.cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.cbar.draw_all()
        self.cits_canvas.show()

    def return_slider_val(self, val):
        self.update_CITS_energy(int(float(val)))

    def extract_pointspectrum(self, scan_dir=0, font_size=18, font_weight='bold', line_width=2, line_style='-', line_colour='blue'):
        self.cits_pointspec = plt.figure()
        self.cits_pointspec_plot = self.cits_pointspec.add_subplot(111)
        assert len(self.v_range) == len(self.cits_data[scan_dir][:, self.y_loc, self.x_loc]), 'No. of points in voltage should match points in cits data'
        self.cits_pointspec_plot.plot(self.v_range, self.cits_data[scan_dir][:, self.y_loc, self.x_loc], linewidth=line_width, linestyle=line_style, c=line_colour)
        self.cits_pointspec_plot.set_xlabel(self.cits_info['unitv'], fontsize=font_size, fontweight=font_weight)
        self.cits_pointspec_plot.set_title(self.cits_info['filename']+'\n x: '+str(self.x_loc)+'  y: '+str(self.y_loc), fontsize=font_size, fontweight=font_weight)
        self.cits_pointspec_plot.set_xlim([self.v_start, self.v_end])
        self.cits_pointspec.show()

    def save_button(self):
        Tkinter.Tk().withdraw()
        self.save_directory = askdirectory()
        self.save_filename = self.cits_info['filename']+'_'+str(np.round(self.v_range[np.int_(self.cits_slider.get())], decimals=3))+'V'
        self.cits_window.savefig(self.save_directory+'/'+self.save_filename+'.png', dpi=400)

    def quit_button(self):
        self.cits_viewer.destroy()
        quit()

"""
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%          Topography Functions         %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
"""

def extract_topo_data(file_data):
    topo_data = []
    for i in range(0, len(file_data)):
        topo_data.append(file_data[i].data)
    return topo_data

def extract_topo_info(file_data):
    topo_info = [{}, {}, {}, {}]
    for i in range(0, len(file_data)):
        topo_info[i]['type'] = file_data[i].info['type']
        assert topo_info[i]['type'] == 'topo', 'file_data should be a topograph'
        topo_info[i]['direction'] = file_data[i].info['direction']
        topo_info[i]['location'] = file_data[i].info['filename']
        topo_info[i]['filename'] = topo_info[i]['location'].split('/')
        topo_info[i]['filename'] = topo_info[i]['filename'][len(topo_info[i]['filename'])-1]
        topo_info[i]['date'] = file_data[i].info['date']
        topo_info[i]['comment'] = file_data[i].info['comment']
        topo_info[i]['xres'] = file_data[i].info['xres']
        topo_info[i]['yres'] = file_data[i].info['yres']
        topo_info[i]['xinc'] = file_data[i].info['xinc']
        topo_info[i]['yinc'] = file_data[i].info['yinc']
        topo_info[i]['xreal'] = file_data[i].info['xreal']
        topo_info[i]['yreal'] = file_data[i].info['yreal']
        topo_info[i]['unitxy'] = file_data[i].info['unitxy']
        topo_info[i]['vgap'] = file_data[i].info['vgap']
        topo_info[i]['iset'] = file_data[i].info['current']
    return topo_info

def plot_topo_ind(file_data, scan_dir=0, color_map='hot', font_size=18, font_weight='bold'):
    topo_window = plt.figure()
    topo_image = topo_window.add_subplot(111)

    topo_info = extract_topo_info(file_data)
    topo_x_extent = topo_info[scan_dir]['xres']*topo_info[scan_dir]['xinc']
    topo_y_extent = topo_info[scan_dir]['yres']*topo_info[scan_dir]['yinc']

    topo_data = extract_topo_data(file_data)

    topo_image_cbarCall = topo_image.imshow(topo_data[scan_dir], origin='lower', extent=[0, topo_x_extent, 0 , topo_y_extent], cmap=color_map, interpolation='none')
    topo_cbar = topo_window.colorbar(topo_image_cbarCall)
    topo_image.set_xlabel(topo_info[scan_dir]['unitxy'], fontsize=font_size, fontweight=font_weight)
    topo_image.set_ylabel(topo_info[scan_dir]['unitxy'], fontsize=font_size, fontweight=font_weight)
    topo_cbar.set_label('m', fontsize=font_size, fontweight=font_weight)
    plt.show()


def topo_dat_plt(topo_data, topo_info, z_scale = 1.25, color_map='hot', font_size=18, font_weight='bold'):
    topo_window = plt.figure()
    topo_image = topo_window.add_subplot(111)

    topo_x_extent = topo_info['xres']*topo_info['xinc']
    topo_y_extent = topo_info['yres']*topo_info['yinc']
    topo_z_min = np.amin(topo_data)
    topo_z_max = np.amax(topo_data)

    topo_image_cbarCall = topo_image.imshow(topo_data, origin='lower', extent=[0, topo_x_extent, 0 , topo_y_extent], vmin = topo_z_min, vmax = topo_z_max*z_scale, cmap=color_map, interpolation='none')
    topo_cbar = topo_window.colorbar(topo_image_cbarCall)
    topo_image.set_xlabel(topo_info['unitxy'], fontsize=font_size, fontweight=font_weight)
    topo_image.set_ylabel(topo_info['unitxy'], fontsize=font_size, fontweight=font_weight)
    topo_cbar.set_label('m', fontsize=font_size, fontweight=font_weight)
    plt.show()

class topo_global_plane():

    def __init__(self, file_data, scan_dir=0):

        self.topo_info = extract_topo_info(file_data)
        self.x_res = self.topo_info[scan_dir]['xres']
        self.y_res = self.topo_info[scan_dir]['yres']

        self.x_range = np.arange(0, self.x_res, 1)
        self.y_range = np.arange(0, self.y_res, 1)

        self.topo_data = extract_topo_data(file_data)[scan_dir]

        self.param_init = [1, 1, 1]

        self.topo_plane_lsq = leastsq(self.topo_plane_residuals, self.param_init, args=self.topo_data)[0]
        self.topo_plane_fit = self.topo_plane_paramEval(self.topo_plane_lsq)
        self.topo_data_flattened = self.topo_data - self.topo_plane_fit
        self.topo_data_flattened = self.topo_data_flattened - np.amin(self.topo_data_flattened)

    def return_flattened_data(self):
        return self.topo_data_flattened

    def topo_plane_residuals(self, param, topo_data):
        self.p_x = param[0]
        self.p_y = param[1]
        self.p_z = param[2]

        self.diff = []
        for y in range(0, self.y_res):
            for x in range(0, self.x_res):
                self.diff.append(topo_data[y, x] - (self.p_x*x + self.p_y*y + self.p_z))
        return self.diff

    def topo_plane_paramEval(self, param):
        self.topo_plane_fit_data = np.zeros((self.y_res, self.x_res))
        for y in range(0, self.y_res):
            for x in range(0, self.x_res):
                self.topo_plane_fit_data[y, x] = param[0]*x + param[1]*y + param[2]
        return self.topo_plane_fit_data


class topo_local_plane():

    def __init__(self, file_data, x0, x1, y0, y1, scan_dir=0):

        self.topo_info = extract_topo_info(file_data)
        self.x_res = self.topo_info[scan_dir]['xres']
        self.y_res = self.topo_info[scan_dir]['yres']

        self.x_range = np.arange(0, self.x_res, 1)
        self.y_range = np.arange(0, self.y_res, 1)

        self.topo_data = extract_topo_data(file_data)
        self.topo_data = self.topo_data[scan_dir]

        self.param_init = [1, 1, 1]

        self.topo_plane_lsq = leastsq(self.topo_plane_residuals, self.param_init, args=(self.topo_data, x0, x1, y0, y1))[0]
        self.topo_plane_fit = self.topo_plane_paramEval(self.topo_plane_lsq)
        self.topo_data_flattened = self.topo_data - self.topo_plane_fit
        self.topo_data_flattened = self.topo_data_flattened - np.amin(self.topo_data_flattened)

    def return_flattened_data(self):
        return self.topo_data_flattened

    def topo_plane_residuals(self, param, topo_data, x0, x1, y0, y1):
        self.p_x = param[0]
        self.p_y = param[1]
        self.p_z = param[2]

        self.diff = []
        for y in range(y0, y1):
            for x in range(x0, x1):
                self.diff.append(topo_data[y, x] - (self.p_x*x + self.p_y*y + self.p_z))
        return self.diff

    def topo_plane_paramEval(self, param):
        self.topo_plane_fit_data = np.zeros((self.y_res, self.x_res))
        for y in range(0, self.y_res):
            for x in range(0, self.x_res):
                self.topo_plane_fit_data[y, x] = param[0]*x + param[1]*y + param[2]
        return self.topo_plane_fit_data


class plot_topo():

    def __init__(self, file_data, scan_dir=0, color_map='hot', font_size=18, font_weight='bold'):
        self.file_data = file_data

        # Create matplotlib figure()
        self.topo_window = plt.figure()

        self.topo_image = self.topo_window.add_subplot(111)

        self.topo_info = extract_topo_info(file_data)
        self.topo_x_extent = self.topo_info[scan_dir]['xres']*self.topo_info[scan_dir]['xinc']
        self.topo_y_extent = self.topo_info[scan_dir]['yres']*self.topo_info[scan_dir]['yinc']

        self.topo_data = extract_topo_data(file_data)

        self.topo_image_cbarCall = self.topo_image.imshow(self.topo_data[scan_dir], origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap=color_map, interpolation='none')
        self.topo_cbar = self.topo_window.colorbar(self.topo_image_cbarCall)
        self.topo_image.set_xlabel(self.topo_info[scan_dir]['unitxy'], fontsize=font_size, fontweight=font_weight)
        self.topo_image.set_ylabel(self.topo_info[scan_dir]['unitxy'], fontsize=font_size, fontweight=font_weight)
        self.topo_cbar.set_label('m', fontsize=font_size, fontweight=font_weight)

        self.topo_viewer = Tkinter.Tk()
        self.topo_canvas = FigureCanvasTkAgg(self.topo_window, master=self.topo_viewer)
        self.topo_canvas.get_tk_widget().grid(column=0, row=1, columnspan=4, sticky='EW')
        self.mclick = self.topo_canvas.mpl_connect('button_press_event', self.mouse_click)
        self.mrelease = self.topo_canvas.mpl_connect('button_release_event', self.mouse_release)

        # Topography Filename
        self.topo_label = Tkinter.Label(self.topo_viewer, text='None', bg='blue', fg='white')
        self.topo_label.configure(text=self.topo_info[scan_dir]['location']+'\n'+self.topo_info[scan_dir]['direction'])
        self.topo_label.grid(column=0, row=0, columnspan=4, sticky='EW')

        # Frame to place buttons for switching between scan directions
        self.topo_dir_buttonPanel = Tkinter.Frame(master=self.topo_viewer)
        self.topo_dir_buttonPanel.grid(column=0, row=3, columnspan=4)

        # Forward Up scan button
        self.topo_fwdup_button = Tkinter.Button(master=self.topo_dir_buttonPanel, text=u'fwd-up', command=self.topo_fwdup)
        self.topo_fwdup_button.grid(column=0, row=0, sticky='EW')
        self.topo_fwdup_button.configure(relief=Tkinter.SUNKEN)

        # Backward Up scan button
        self.topo_bwdup_button = Tkinter.Button(master=self.topo_dir_buttonPanel, text=u'bwd-up', command=self.topo_bwdup)
        self.topo_bwdup_button.grid(column=1, row=0, sticky='EW')

        # Forward Down scan button
        self.topo_fwddwn_button = Tkinter.Button(master=self.topo_dir_buttonPanel, text=u'fwd-down', command=self.topo_fwddwn)
        self.topo_fwddwn_button.grid(column=2, row=0, sticky='EW')

        # Backward Down scan button
        self.topo_bwddwn_button = Tkinter.Button(master=self.topo_dir_buttonPanel, text=u'bwd-down', command=self.topo_bwddwn)
        self.topo_bwddwn_button.grid(column=3, row=0, sticky='EW')

        # Frame to hold image analysis tools
        self.topo_analysis_frame = Tkinter.Frame(master=self.topo_viewer)
        self.topo_analysis_frame.grid(column=0, row=4, columnspan=4)

        # Global Plane Flatten Tool Button
        self.topo_global_plane_button = Tkinter.Button(master=self.topo_analysis_frame, text=u'Global Plane', command=self.topo_global_plane_cmd)
        self.topo_global_plane_button.grid(column=0, row=0)

        # Local Plane Flatten Tool Button
        self.topo_local_plane_button = Tkinter.Button(master=self.topo_analysis_frame, text=u'Local Plane', command=self.topo_local_plane_cmd)
        self.topo_local_plane_button.grid(column=1, row=0)

        # Save currently displayed topo
        self.topo_save_button = Tkinter.Button(master=self.topo_viewer, text=u'Save Topo', command=self.get_topo_data)
        self.topo_save_button.grid(column=0, row=5, sticky='W')

        # Cleanly exit Topo viewer
        self.topo_quit_button = Tkinter.Button(master=self.topo_viewer, text=u'QUIT', command=self.topo_quit)
        self.topo_quit_button.grid(column=3, row=5, sticky='E')


        # Define Window Geometry
        self.screen_w = self.topo_viewer.winfo_screenwidth()
        self.screen_h = self.topo_viewer.winfo_screenheight()
        self.topo_viewer.grid_columnconfigure(0, weight=1)
        self.topo_viewer.resizable(True, True)
        self.topo_viewer.geometry('%dx%d+%d+%d' % (0.41*self.screen_w, 0.73*self.screen_h, 0.05*self.screen_w, 0.05*self.screen_h))
        self.topo_viewer.update_idletasks()
        self.topo_canvas.show()

        self.scan_dir = scan_dir
        self.x_loc_click = 0
        self.x_loc_release = self.topo_info[scan_dir]['xres']-1
        self.y_loc_click = 0
        self.y_loc_release = self.topo_info[scan_dir]['yres']-1

        Tk.mainloop(self.topo_viewer)

    def mouse_click(self, event):
        self.x_res_graph = self.topo_info[self.scan_dir]['xinc']
        self.y_res_graph = self.topo_info[self.scan_dir]['yinc']
        self.x_inst = event.xdata
        self.y_inst = event.ydata
        if isinstance(event.xdata, float):
            if isinstance(event.ydata, float):
                self.x_loc_click = int(self.x_inst / self.x_res_graph)
                self.y_loc_click = int(self.y_inst / self.y_res_graph)
            else:
                self.x_loc_click = 0
                self.y_loc_click = 0

    def mouse_release(self, event):
        self.x_res_graph = self.topo_info[self.scan_dir]['xinc']
        self.y_res_graph = self.topo_info[self.scan_dir]['yinc']
        self.x_inst = event.xdata
        self.y_inst = event.ydata
        if isinstance(event.xdata, float):
            if isinstance(event.ydata, float):
                self.x_loc_release = int(self.x_inst / self.x_res_graph)
                self.y_loc_release = int(self.y_inst / self.y_res_graph)
            else:
                self.x_loc_click = self.topo_info['xres']-1
                self.y_loc_click = self.topo_info['yres']-1

    def topo_local_plane_cmd(self):

        self.x0 = np.amin([self.x_loc_click, self.x_loc_release])
        self.x1 = np.amax([self.x_loc_click, self.x_loc_release])
        self.y0 = np.amin([self.y_loc_click, self.y_loc_release])
        self.y1 = np.amax([self.y_loc_click, self.y_loc_release])

        self.local_plane_data = topo_local_plane(self.file_data, self.x0, self.x1, self.y0, self.y1, scan_dir=self.scan_dir).return_flattened_data()
        self.topo_data[self.scan_dir] = self.local_plane_data
        self.topo_image.imshow(self.local_plane_data, origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap='hot', interpolation='none')
        self.img_min = np.amin(self.local_plane_data)
        self.img_max = np.amax(self.local_plane_data)
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_label.configure(text=self.topo_info[0]['location']+'\n'+self.topo_info[0]['direction']+'_localPlane')
        self.topo_cbar.draw_all()
        self.topo_canvas.show()

    def topo_global_plane_cmd(self):

        self.global_plane_data = topo_global_plane(self.file_data, scan_dir=self.scan_dir).return_flattened_data()
        self.topo_data[self.scan_dir] = self.global_plane_data
        self.topo_image.imshow(self.global_plane_data, origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap='hot', interpolation='none')
        self.img_min = np.amin(self.global_plane_data)
        self.img_max = np.amax(self.global_plane_data)
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_label.configure(text=self.topo_info[0]['location']+'\n'+self.topo_info[0]['direction']+'_globalPlane')
        self.topo_cbar.draw_all()
        self.topo_canvas.show()

    def topo_fwdup(self, color_map='hot'):
        self.topo_image.imshow(self.topo_data[0], origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap=color_map, interpolation='none')
        self.img_min = np.amin(self.topo_data[0])
        self.img_max = np.amax(self.topo_data[0])
        self.topo_label.configure(text=self.topo_info[0]['location']+'\n'+self.topo_info[0]['direction'])
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_cbar.draw_all()
        self.topo_canvas.show()
        self.topo_fwdup_button.configure(relief=Tkinter.SUNKEN)
        self.topo_bwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_fwddwn_button.configure(relief=Tkinter.RAISED)
        self.topo_bwddwn_button.configure(relief=Tkinter.RAISED)
        self.scan_dir = 0

    def topo_bwdup(self, color_map='hot'):
        self.topo_image.imshow(self.topo_data[1], origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap=color_map, interpolation='none')
        self.img_min = np.amin(self.topo_data[1])
        self.img_max = np.amax(self.topo_data[1])
        self.topo_label.configure(text=self.topo_info[1]['location']+'\n'+self.topo_info[1]['direction'])
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_cbar.draw_all()
        self.topo_canvas.show()
        self.topo_fwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_bwdup_button.configure(relief=Tkinter.SUNKEN)
        self.topo_fwddwn_button.configure(relief=Tkinter.RAISED)
        self.topo_bwddwn_button.configure(relief=Tkinter.RAISED)
        self.scan_dir = 1

    def topo_fwddwn(self, color_map='hot'):
        self.topo_image.imshow(self.topo_data[2], origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap=color_map, interpolation='none')
        self.img_min = np.amin(self.topo_data[2])
        self.img_max = np.amax(self.topo_data[2])
        self.topo_label.configure(text=self.topo_info[2]['location']+'\n'+self.topo_info[2]['direction'])
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_cbar.draw_all()
        self.topo_canvas.show()
        self.topo_fwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_bwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_fwddwn_button.configure(relief=Tkinter.SUNKEN)
        self.topo_bwddwn_button.configure(relief=Tkinter.RAISED)
        self.scan_dir = 2

    def topo_bwddwn(self, color_map='hot'):
        self.topo_image.imshow(self.topo_data[3], origin='lower', extent=[0, self.topo_x_extent, 0, self.topo_y_extent], cmap=color_map, interpolation='none')
        self.img_min = np.amin(self.topo_data[3])
        self.img_max = np.amax(self.topo_data[3])
        self.topo_label.configure(text=self.topo_info[3]['location']+'\n'+self.topo_info[3]['direction'])
        self.topo_cbar.set_clim(vmin=self.img_min, vmax=self.img_max)
        self.topo_cbar.draw_all()
        self.topo_canvas.show()
        self.topo_fwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_bwdup_button.configure(relief=Tkinter.RAISED)
        self.topo_fwddwn_button.configure(relief=Tkinter.RAISED)
        self.topo_bwddwn_button.configure(relief=Tkinter.SUNKEN)
        self.scan_dir = 3

    def get_topo_data(self, scan_dir=0):
        return self.topo_data[scan_dir]

    def topo_quit(self):
        self.topo_viewer.destroy()
        quit()

def topo_show(data_location, filename, z_scale=1.25):

    loc = data_location
    fn = filename

    file_locations = loc+filename+'.Z_flat'
    file_topo_filedata = import_data(file_locations)
    file_topo_info = extract_topo_info(file_topo_filedata)
    file_topo_data = extract_topo_data(file_topo_filedata)
    file_data_gplane = topo_global_plane(file_topo_filedata).return_flattened_data()
    return topo_dat_plt(file_data_gplane, file_topo_info[0], z_scale=z_scale)

"""
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %%%         Spectroscopy Functions        %%%
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
"""

def extract_spec_data(file_data):
    spec_data = []
    for i in range(0, len(file_data)):
        spec_data.append(file_data[i].data)
    return spec_data

def extract_spec_info(file_data):
    spec_info = [{},{}]
    for i in range(0, len(file_data)):
        spec_info[i]['type'] = file_data[i].info['type']
        assert spec_info[i]['type'] == 'ivcurve', 'file_data is not ivcurve'
        spec_info[i]['direction'] = file_data[i].info['direction']
        spec_info[i]['location'] = file_data[i].info['filename']
        spec_info[i]['filename'] = spec_info[i]['location'].split('/')
        spec_info[i]['filename'] = spec_info[i]['filename'][len(spec_info[i]['filename'])-1]
        spec_info[i]['date'] = file_data[i].info['date']
        spec_info[i]['comment'] = file_data[i].info['comment']
        spec_info[i]['vres'] = file_data[i].info['vres']
        spec_info[i]['vstart'] = file_data[i].info['vstart']
        spec_info[i]['vinc'] = file_data[i].info['vinc']
        spec_info[i]['vreal'] = file_data[i].info['vreal']
        spec_info[i]['unitv'] = file_data[i].info['unitv']
        spec_info[i]['iset'] = file_data[i].info['current']
    return spec_info

def plot_spec_ind(file_data, scan_dir=0, font_size=18, font_weight='bold'):
    spec_window = plt.figure()
    spec_image_trace = spec_window.add_subplot(111)
    spec_image_retrace = spec_window.add_subplot(111)

    spec_info = extract_spec_info(file_data)
    spec_v_range = np.arange(spec_info[scan_dir]['vstart'], spec_info[scan_dir]['vstart'] + spec_info[scan_dir]['vres']*spec_info[scan_dir]['vinc'], spec_info[scan_dir]['vinc'])

    spec_data = extract_spec_data(file_data)

    spec_image_trace.plot(spec_v_range, spec_data[scan_dir])
    spec_image_retrace.plot(spec_v_range, spec_data[scan_dir + 1])
    spec_image_trace.set_xlabel(spec_info[scan_dir]['unitv'], fontsize=font_size, fontweight=font_weight)
    spec_image_trace.set_ylabel('dI/dV (arb.)', fontsize=font_size, fontweight=font_weight)
    plt.show()


def multiple_spec_avg(spec_data):

    spec_number = len(spec_data)
    spec_length = len(spec_data[0])

    for i in range(0, spec_number):
        if len(spec_data[i]) == spec_length:
            None
        else:
            print 'All Spectra Must Be Same Length'
            return

    all_spectra = np.empty([spec_number, spec_length])
    for i in range(0, spec_number):
        all_spectra[i] = spec_data[i]

    avg_spectra = np.empty([spec_length])
    for i in range(0, spec_length):
        avg_spectra[i] = np.sum(all_spectra[:, i])/spec_number
    return avg_spectra


def plt_spec(data_location, filename, number=1, showplt='y', returnall='n'):

    no = number
    loc = data_location
    file_locations = []
    for i in range(0, no):
        file_locations.append(loc+filename+str(i+1)+'.Aux1(V)_FLAT')

    file_filedata = []
    for i in range(0, no):
        file_filedata.append(import_data(file_locations[i]))

    file_info = extract_spec_info(file_filedata[0])

    file_data = []
    for i in range(0, no):
        temp_import = extract_spec_data(file_filedata[i])
        for j in range(0, 2):
            file_data.append(temp_import[j])
    file_data_avg = multiple_spec_avg(file_data)

    file_vstart = file_info[0]['vstart']
    file_vres = file_info[0]['vres']
    file_vinc = file_info[0]['vinc']
    file_vrange = np.arange(file_vstart, file_vstart+file_vres*file_vinc, file_vinc)

    if showplt == 'y':

        file_plt_window = plt.figure()
        file_plt_ax = file_plt_window.add_subplot(111)
        for i in range(0, 2*no):
            file_plt_ax.plot(file_vrange, file_data[i], linewidth=1, color='black')
        file_plt_ax.plot(file_vrange, file_data_avg, linewidth=3, color='red')
        file_plt_ax.set_xlim([file_vstart+file_vres*file_vinc, file_vstart])
        file_plt_ax.grid(b='True', which='major', color='k', linestyle='-')
        file_plt_ax.set_xlabel(file_info[0]['unitv'])
        file_plt_ax.set_ylabel('dI/dV (arb.)')
        plt.show()
        return file_data_avg, file_vrange
    elif showplt =='n':
        if returnall =='n':
            return file_data_avg, file_vrange
        elif returnall == 'y':
            return file_data_avg, file_vrange, file_data
    else:
        print 'Unknown Request'

def lockin2real(num, v_mod, v_sen, resistance=3*10**9, rescale_factor=1):
    return num*rescale_factor*(v_sen/10)*(1/(v_mod*resistance))